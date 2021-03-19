from pyflow import *


# @task
# def fq2fa(fq: File):
#     """
#     fq2fa {fq} > {set_output}
#     """
#     prefix = str(fq).rstrip(".fq")
#
#     return "{prefix}.fa"
#
#
# @task
# def blast(fa: File, db: File):
#     """
#     blastn -db {db} -in {fa} -out {set_output}
#     """
#     prefix = str(fa).rstrip('.fa')
#
#     return "{prefix}.txt"
#
#
# @task
# def count_lines(fa: File) -> int:
#     with open(fa) as f:
#         return len(f.readlines())
#
#
# @flow
# def test_flow(fa: File, db: File) -> ""
#     fq2fa(fq)
#     blast_results = blast(fa, db)  # both is ok
#     blast_results = blast(fq2fa.out, db)

# def shell(**kwargs):
#     return fn
#
#
class _(object):
    def __init__(self, *args, **kwargs):
        pass


@conda("samtools pybigwig")
@shell
def bwa_map(fa, fastq):
    bam = 'a.bam'
    _(f"bwa mem {fa} {fastq} | samtools view -Sb > {bam}")

    return bam


@docker("ubuntu:latest")
@shell(
    cpu=10,
    mem="10G",
    priority=2
)
def samtools_sort(bam: File):
    sorted_bam = 'a.bam'
    _(f"""samtools sort -T sorted -O bam {bam} > {sorted_bam}""")
    return sorted_bam


@shell
def bcftools_call(fa, bam):
    vcf = 'a.vcf'
    _(f"""samtools mpileup -g -f {fa} {bam} | bcftools call -mv - > {vcf}""")
    return vcf


@flow
def pipeline(fa, fastq):
    bam = bwa_map(fa, fastq)
    sorted_bam = samtools_sort(bam)
    vcf = bcftools_call(fa, sorted_bam)

    return vcf


fa = Channel.value('1.fa')
samples = Channel.from_file_pairs("*_{1, 2}.fastq")

FlowRunner(pipeline).run(fa, samples)
