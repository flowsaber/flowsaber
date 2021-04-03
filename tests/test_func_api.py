import sys

sys.path.insert(0, '../')

import flowsaber
from flowsaber import *
import uuid
import numpy as np

EnvTask.DEFAULT_CONFIG = {'workdir': '/tmp/Env'}  # make the EnvTask cache at a global place


def test_flow1():
    @task
    def bwa(fasta):
        return str(fasta) + ".bam"

    @task
    def stat(bam):
        return bam + ".bigiwg"

    @task
    def mod(bw1):
        return f"{bw1}.txt"

    @flow
    def flow1(fasta):
        bam = bwa(fasta)
        return stat(bam)

    @flow
    def flow2(bw1):
        return mod(mod(bw1))

    @flow
    def myflow(fasta1, fasta2):
        bw1 = flow1(fasta1)
        bw2 = flow1(fasta2)
        bw12 = merge(bw1, bw2).map(by=lambda bws: '-'.join(bws))
        txt = flow2(bw12).view() \
            .subscribe(on_next=lambda x: print(f"The value is {x}"), on_complete=lambda: print("Now reach the END")) \
            .map(by=lambda x: x + x) \
            .concat(Channel.values('5', '6', '7', 8, 9, 10))
        a = Channel.from_list([1, 2, 3, 4]).mix(txt)
        m = merge(a, a, a)
        outputs = m >> [mod, flow1, flow2, flow1]
        return merge(*outputs) | flatten | [mod, bwa, stat, flow1, flow2, flow1] | split(6) | merge

    fasta1 = Channel.values("1", "2", "4")
    fasta2 = Channel.values("A", "B", "x", "a")

    workflow = myflow(fasta1, fasta2)
    consumer = Consumer.from_channels(workflow._output)
    asyncio.run(flowsaber.run(workflow))

    results = []
    for data in consumer:
        results.append(data)
    print("Results are: ")
    for res in results:
        print(res, type(res))

    workflow.graph.render('class_dag1', view=False, format='pdf', cleanup=True)


def test_flow2():
    @task
    def comput1(a, b):
        l = len(str(b))
        ma = np.random.randn(l, l)
        return {a: [1] * l, b: ma}

    @task
    def comput2(self, dic):
        return '-'.join(str(k) for k in dic.keys())

    @shell(conda="bwa samtools")
    def shell1(self, f: str):
        Shell(f"echo  '{f}' > {f}\n"
              f"pwd\n"
              f"ls -lah")
        return f

    # @shell(image="docker://continuumio/miniconda", pubdir="results/shell2")
    @shell(pubdir="results/shell2")
    def shell2(self, f: File):
        f1 = "t1.txt"
        f2 = "t2.txt"
        Shell(f"""
               cat  '{f}' >> {f1}
               cat '{f1}' >> {f2}
               cat '{f}' >> {f2}
               pwd
              """)
        return f1, f2

    # @shell(conda="samtools bwa python", image="docker://continuumio/miniconda")
    @shell(conda="samtools bwa python")
    def shell3(f: File):
        Shell(f"cat {f}")

    @flow
    def flow1(self, ch1, ch2):
        dict_ch = comput1(ch1, ch2)
        return comput2(dict_ch)

    @flow
    def flow2(self, ch):
        fch = shell1(ch) | subscribe(lambda x: print(f"{shell1} output {x}"))
        f12 = shell2(fch)
        f1ch, f2ch = Split(num=2)(f12)
        return concat(f1ch, f2ch) | shell3

    @flow
    def myflow(a, b):
        ch = flow1(a, b) | view
        return flow2(ch)

    fasta1_list = [uuid.uuid4() for i in range(10)]
    fasta2_list = [uuid.uuid4() for i in range(10)]

    fasta1 = Channel.values(*fasta1_list)
    fasta2 = Channel.values(*fasta2_list)

    config.update({
        'cpu': 10,
        'memory': 100,
        'time': 1000,
        'io': 80
    })

    workflow = myflow(fasta1, fasta2)
    consumer = Consumer.from_channels(workflow._output)
    asyncio.run(flowsaber.run(workflow))

    results = []
    for data in consumer:
        results.append(data)
    print("Results are: ")
    for res in results:
        print(res, type(res))

    workflow.graph.render('class_dag2', view=False, format='pdf', cleanup=True)


if __name__ == "__main__":
    test_flow1()
    print("-" * 40)
    test_flow2()
