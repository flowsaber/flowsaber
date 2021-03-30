import sys

sys.path.insert(0, '../')
from pyflow import *

# EnvTask is the real dependent task when using conda/image option
EnvTask.DEFAULT_CONFIG = {'workdir': '/tmp/Env'}  # make the EnvTask cache at a global place


@shell(conda="bwa=0.7.17 samtools=1.9")
def bwa(self, fa: File, fastq: File):  # input will be automatically converted if has type annotation
    """bwa mem -t {self.cpu} {fa} {fastq} | samtools view -Sb - > {fastq.stem}.bam"""
    return "*.bam"  # for ShellTask, str variable in the return will be treated as File and globed


@shell(conda="bwa=0.7.17 samtools=1.9")
def sort(bam: File):  # self is optional in case you don't want to access the current task
    """samtools sort -o {bam.stem}.sorted.bam {bam}"""
    return "*.sorted.bam"


@shell(conda="bcftools=1.9 samtools=1.9", pubdir="results/vcf")
def call(fa: File, bams: list):  # In case you need to write some python codes
    bams = ' '.join(str(bam) for bam in bams)
    Shell(f"samtools mpileup -g -f {fa} {bams} | bcftools call -mv - > all.vcf")
    return "all.vcf"


@task(pubdir="results/stats")
def stats(vcf: File):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from pysam import VariantFile

    quals = [record.qual for record in VariantFile(vcf.open('rb'))]
    plt.hist(quals)

    plt.savefig("report.svg")


@flow
def call_vcf_flow(fa, fastq):
    def _call(bams):  # task is normal function, use python as wish
        return call(fa, bams)

    bam1 = bwa(fa, fastq)  # automatically clone channel
    bam2 = bwa(fa, fastq)
    mix(bam1, bam2) | sort | collect | _call | stats


config.update({
    'cpu': 8,
    'memory': 20,
})

prefix = 'snamke-demo.nosync/data'
fa = Channel.value(f'{prefix}/genome.fa')
fastq = Channel.values(*[f'{prefix}/samples/{sample}' for sample in ['A.fastq', 'B.fastq', 'C.fastq']])

runner, workflow = FlowRunner(call_vcf_flow).run(fa, fastq)
# generate dag
workflow.graph.render('/tmp/workflow', view=True, format='png', cleanup=True)
# run the flow
runner.execute()
