from pyflow import *
import numpy as np


def test():
    class Comput1(Task):
        def run(self, a, b):
            l = len(str(b))

            ma = np.random.randn(l, l)
            return {a: [1] * l, b: ma}

    class Comput2(Task):
        def run(self, dic):
            return '-'.join([str(k) for k in dic.keys()])

    class Shell1(ShellTask):
        def command(self, f: str):
            Shell(f"echo  '{f}' > {f}")
            return f

    class Shell2(ShellTask):
        def command(self, f: File):
            f1 = "t1.txt"
            f2 = "t2.txt"
            Shell(f"""
               cat  '{f}' >> {f1}
               cat '{f1}' >> {f2}
               cat '{f}' >> {f2}
              """)
            return f1, f2

    class Shell3(ShellTask):
        def command(self, f: File) -> str:
            Shell(f"cat {f}")

    comput1 = Comput1()
    comput2 = Comput2()

    shell1 = Shell1()
    shell2 = Shell2()
    shell3 = Shell3(workdir='/tmp')

    class Flow1(Flow):
        def run(self, ch1, ch2):
            dict_ch = comput1(ch1, ch2)
            return comput2(dict_ch)

    class Flow2(Flow):
        def run(self, ch):
            fch = shell1(ch)
            f12 = shell2(fch)
            f1ch, f2ch = Split(num=2)(f12)
            return concat(f1ch, f2ch) | shell3

    flow1 = Flow1()
    flow2 = Flow2()

    class Flow12(Flow):
        def run(self, a, b):
            ch = flow1(a, b)
            return flow2(ch)

    myflow = Flow12()

    fasta1 = Channel.values("1", "2", "4", "1")
    fasta2 = Channel.values("A", "B", "x", "A")

    runner, workflow = FlowRunner(myflow).run(fasta1, fasta2)
    consumer = Consumer.from_channels(workflow._output)
    runner.execute()
    results = []
    for data in consumer:
        results.append(data)
    print("Results are: ")
    for res in results:
        print(res, type(res))

    # workflow.graph.render('/Users/bakezq/Desktop/dag', view=True, format='pdf', cleanup=True)


if __name__ == "__main__":
    test()
