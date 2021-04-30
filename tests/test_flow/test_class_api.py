from flowsaber.api import *


def test_flow1():
    class Bwa(Task):
        def run(self, fasta):
            return str(fasta) + ".bam"

    class Stat(Task):
        def run(self, bam):
            return bam + ".bigwig"

    class Mod(Task):
        def run(self, bw1):
            return f"{bw1}.txt"

    bwa = Bwa()
    stat = Stat()
    mod = Mod()

    class MyFlow1(Flow):
        def run(self, fasta: Channel) -> Channel:
            bam = bwa(fasta)
            return stat(bam)

    class MyFLow2(Flow):
        def run(self, bw1: Channel) -> Channel:
            return mod(mod(bw1))

    flow1 = MyFlow1()
    flow2 = MyFLow2()

    class MyFlow(Flow):
        def run(self, fasta1: Channel, fasta2: Channel):
            bw1 = flow1(fasta1)
            bw2 = flow1(fasta2)
            bw12 = merge(bw1, bw2).map(by=lambda bws: '-'.join(bws))
            txt = flow2(bw12).view() \
                .subscribe(on_next=lambda x: print(f"The value is {x}"), on_complete=lambda: print("Now reach the END")) \
                .map(by=lambda x: x + x) \
                .concat(Channel.values('5', '6', '7', 8, 9, 10))
            a = Channel.from_list([1, 2, 3, 4]).mix(txt)
            m = merge(a, a, a, a, a)
            outputs = m >> [mod, flow1, flow2, flow1]
            return merge(*outputs).flatten() >> [mod, bwa, stat, flow1, flow2, flow1] | merge | view

    myflow = MyFlow()

    fasta1 = Channel.values("1", "2", "4")
    fasta2 = Channel.values("A", "B", "x", "a")

    workflow = myflow(fasta1, fasta2)
    run(workflow)


def test_flow2():
    class Comput1(Task):
        def run(self, a, b):
            import numpy as np
            l = len(str(b))
            ma = np.random.randn(l, l)

            return {a: [1] * l, b: ma}

    class Comput2(Task):
        def run(self, dic):
            return '-'.join(str(k) for k in dic.keys())

    class Shell1(ShellFlow):
        def command(self, f: str):
            """echo '{f}' > {f}"""
            return f

    class Shell2(ShellFlow):
        def command(self, f: File):
            f1 = "t1.txt"
            f2 = "t2.txt"
            CMD = f"""
               cat  '{f}' >> {f1}
               cat '{f1}' >> {f2}
               cat '{f}' >> {f2}
              """
            return f1, f2

    class Shell3(ShellFlow):
        def command(self, f: File):
            """cat {f}"""

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
            return concat(f1ch, f2ch) | shell3 | shell3

    flow1 = Flow1()
    flow2 = Flow2()

    class Flow12(Flow):
        def run(self, a, b):
            ch = flow1(a, b)
            return flow2(ch) | view

    myflow = Flow12()

    fasta1_list = [uuid.uuid4() for i in range(5)]
    fasta2_list = [uuid.uuid4() for i in range(5)]

    fasta1 = Channel.values(*fasta1_list)
    fasta2 = Channel.values(*fasta2_list)
    workflow = myflow(fasta1, fasta2)
    run(workflow)


if __name__ == "__main__":
    test_flow1()
    print("-" * 40)
    test_flow2()
