import sys
sys.path.insert(0, '../')
from pyflow import *


@shell(
    conda="bwa=0.7.17 samtools=1.9"
)
def bwa(self, fa: File, fastq: File):
    bam = f"{fastq.stem}.bam"
    Shell(f"bwa mem -t {self.cpu} {fa} {fastq} | samtools view -Sb - > {bam}")
    return bam


@shell(
    conda="bwa=0.7.17 samtools=1.9"
)
def sort(bam: File):
    sorted_bam = f"{bam.stem}.sorted.bam"
    Shell(f"samtools sort -o {sorted_bam} {bam}")
    return sorted_bam


@shell(
    conda="bcftools=1.9 samtools=1.9"
)
def call(fa: File, bams: list):
    bams = ' '.join(str(bam) for bam in bams)
    Shell(f"samtools mpileup -g -f {fa} {bams} |"
          f"bcftools call -mv - > all.vcf")
    return "*.vcf"


@task(
    pubdir="results/stats"
)
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
    bams = bwa(fa, fastq) | sort | collect
    call(fa, bams) | stats


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
