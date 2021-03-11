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
            b, b2 = a.clone()
            c, d, e = b.clone(num=3)
            m = merge(c, d, e)
            outputs = m >> [mod, flow1, flow2, flow1]
            return merge(*outputs).flatten() >> [mod, bwa, stat, flow1, flow2, flow1] | Split(num=6)

    myflow = MyFlow()

    fasta1 = Channel.values("1", "2", "4")
    fasta2 = Channel.values("A", "B", "x", "a")

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

    workflow.graph.render('/Users/bakezq/Desktop/dag', view=True, format='pdf', cleanup=True)


if __name__ == "__main__":
    test_flow()
