from pyflow.core.task import _
from pyflow import *
import numpy as np


def test():
    @task
    def comput1(a, b):
        l = len(str(b))
        ma = np.random.randn(l, l)
        return {a: [1] * l, b: ma}

    @task
    def comput2(self, dic):
        print(self.name)
        return '-'.join([str(k) for k in dic.keys()])

    @shell
    def shell1(f: str):
        _(f"echo  '{f}' > {f}")
        return f

    @shell
    def shell2(self, f: File):
        print(self.task_key)
        f1 = "t1.txt"
        f2 = "t2.txt"
        _(f"""
               cat  '{f}' >> {f1}
               cat '{f1}' >> {f2}
               cat '{f}' >> {f2}
              """)
        return f1, f2

    @shell(workdir='/tmp')
    def shell3(f: File):
        _(f"cat {f}")

    @flow
    def flow1(self, ch1, ch2):
        dict_ch = comput1(ch1, ch2)
        return comput2(dict_ch)

    @flow
    def flow2(self, ch):
        fch = shell1(ch)
        f12 = shell2(fch)
        f1ch, f2ch = Split(num=2)(f12)
        return concat(f1ch, f2ch) | shell3

    @flow
    def myflow(a, b):
        ch = flow1(a, b)
        return flow2(ch)

    fasta1 = Channel.values("1", "2", "4", "1")
    fasta2 = Channel.values("A", "B", "x", "A")

    workflow = FlowRunner(myflow).run(fasta1, fasta2)
    results = []
    while not workflow.output.empty():
        item = workflow.output.get_nowait()
        if item is END:
            break
        results.append(item)
    print("Results are: ")
    for res in results:
        print(res, type(res), len(res))

    # workflow.graph.render('/Users/bakezq/Desktop/dag', view=True, format='pdf', cleanup=True)


if __name__ == "__main__":
    test()
