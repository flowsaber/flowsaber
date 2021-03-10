# from pyflow import *
#
#
# def test_flow():
#     class Bwa(Task):
#         def run(self, fasta):
#             return str(fasta) + ".bam"
#
#     class Stat(Task):
#         def run(self, bam):
#             return bam + ".bigwig"
#
#     class Mod(Task):
#         def run(self, bw1):
#             return f"{bw1}.txt"
#
#     bwa = Bwa()
#     stat = Stat()
#     mod = Mod()
#
#     with Flow() as flow1:
#         fasta = Channel.from_list([1, 2, 3, 4])
#         bam = bwa(fasta)
#         bigwig = stat(bam)
#         flow1.output = bigwig
#
#     with Flow() as flow2:
#         bw1 = flow2.inputs
#         flow2.output = mod(mod(bw1))
#
#     with Flow() as flow:
#         fasta1, fasta2 = flow.inputs
#         bw1 = flow1(fasta1)
#         bw2 = flow2(fasta2)
#         flow.output = bw2
#
#
# if __name__ == "__main__":
#     test_flow()
