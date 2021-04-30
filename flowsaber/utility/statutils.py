import time
from threading import Thread, Event
from typing import Sequence

import psutil


class ResourceMonitor(Thread):
    """A resource monitor that starts another thread for monitoring resource usages within the `with` scope.
    """
    STATIC_ATTRS = ['num_ctx_switches', 'cpu_times']
    # STATIC_ATTRS = ['num_ctx_switches', 'io_counters', 'cpu_times']
    DYNAMIC_ATTRS = ['cpu_percent', 'num_threads', 'num_fds', 'memory_percent', 'memory_full_info']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = Event()
        self.d_usage = {}
        self.s_usage = {}
        self.num_count = 0
        self.usage = {}

    def __enter__(self):
        self.running.set()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.running.clear()
        self.join()

    def run(self) -> None:
        self.s_usage = self.accum_resource(self.STATIC_ATTRS, self.s_usage)

        while self.running.is_set():
            time.sleep(1)
            self.accum_resource(self.DYNAMIC_ATTRS, self.d_usage)
            self.num_count += 1

        if self.num_count:
            for k, v in self.d_usage.items():
                self.d_usage[k] /= self.num_count

        s_usage = self.accum_resource(self.STATIC_ATTRS)
        for k, v in self.s_usage.items():
            s_usage[k] -= self.s_usage.get(k, 0)

        self.s_usage = s_usage

        self.usage = {**self.s_usage, **self.d_usage}

    @classmethod
    def accum_resource(cls, attrs: Sequence, usage=None):
        # TODO this method may cause errors sometimes, possibly due to quick exit of process.
        try:
            ps = [psutil.Process()]
            ps += list(ps[0].children(recursive=True))
            if usage is None:
                usage = {}

            for p in ps:
                p_usage = cls.unwrap(p.as_dict(attrs))
                for k, v in p_usage.items():
                    usage[k] = usage.get(k, 0) + v
            return usage
        except Exception:
            return {} if usage is None else usage

    @staticmethod
    def unwrap(value: dict):
        res = {}
        for k, v in value.items():
            if isinstance(v, (int, float)):
                res[k] = v
            elif hasattr(v, '__slots__') and len(v.__slots__):
                for sk in v.__slots__:
                    res[sk] = getattr(v, sk)
            elif hasattr(v, '__dict__') and len(v.__dict__):
                res.update(v.__dict__)
            elif hasattr(v, '_asdict'):
                res.update(v._asdict())
        return res
