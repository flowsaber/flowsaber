from pyflow import *


def test_flow():
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
            bigwig = stat(bam)
            return bigwig

    class MyFLow2(Flow):
        def run(self, bw1: Channel) -> Channel:
            return mod(mod(bw1))

    myflow1 = MyFlow1()
    myflow2 = MyFLow2()

    class MyFlow(Flow):
        def run(self, fasta1: Channel, fasta2: Channel):
            bw1 = myflow1(fasta1)
            bw2 = myflow1(fasta2)
            bw12 = merge(bw1, bw2).map(lambda bws: '-'.join(bws))
            txt = myflow2(bw12).view() \
                .subscribe(lambda x: print(f"The value is {x}"), lambda: print("Now reach the END")) \
                .map(lambda x: x + x) \
                .concat(Channel.values('5', '6', '7', 8, 9, 10))
            a = Channel.from_list([1, 2, 3, 4]).mix(txt)
            b = a.clone()
            c, d, e = b.clone(3)
            m = merge(c, d, e)
            outputs = m >> [mod, myflow1, myflow2, myflow1]
            return merge(*outputs).flatten() | [mod, myflow1, myflow2]

    myflow = MyFlow()

    fasta1 = Channel.values("1", "2", "4")
    fasta2 = Channel.values("A", "B", "x", "a")

    results = FlowRunner(myflow).run(fasta1, fasta2)
    print("Results are: ")
    for res in results:
        print(res)


if __name__ == "__main__":
    test_flow()
