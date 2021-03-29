from pyflow import *
from pyflow.context import config
import uuid
import numpy as np


def test():
    @task
    def comput1(a, b):
        l = len(str(b))
        ma = np.random.randn(l, l)
        return {a: [1] * l, b: ma}

    @task
    def comput2(self, dic):
        return '-'.join([str(k) for k in dic.keys()])

    @shell(conda="bwa samtools")
    def shell1(f: str):
        Shell(f"echo  '{f}' > {f}")
        return f

    @shell(image="docker://continuumio/miniconda", pubdir="results/shell2")
    def shell2(self, f: File):
        f1 = "t1.txt"
        f2 = "t2.txt"
        Shell(f"""
               cat  '{f}' >> {f1}
               cat '{f1}' >> {f2}
               cat '{f}' >> {f2}
              """)
        return f1, f2

    @shell(conda="samtools bwa python", image="docker://continuumio/miniconda")
    def shell3(f: File):
        Shell(f"cat {f}")

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

    fasta1_list = [uuid.uuid4() for i in range(100)]
    fasta2_list = [uuid.uuid4() for i in range(100)]

    fasta1 = Channel.values(*fasta1_list)
    fasta2 = Channel.values(*fasta2_list)

    config.update({
        'cpu': 40,
        'memory': 100,
        'time': 1000,
        'io': 80
    })


    runner, workflow = FlowRunner(myflow).run(fasta1, fasta2)
    consumer = Consumer.from_channels(workflow._output)
    runner.execute()
    results = []
    for data in consumer:
        print(data)
        results.append(data)
    print("Results are: ")
    for res in results:
        print(res, type(res))


    workflow.graph.render('/store/qzhong/dag.pdf', view=True, format='pdf', cleanup=True)


if __name__ == "__main__":
    test()
