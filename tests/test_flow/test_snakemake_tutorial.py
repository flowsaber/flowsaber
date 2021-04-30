from flowsaber.api import *


def test_snakemake_workflow():
    # EnvTask is the real dependent task when using conda/image option

    @shell
    def bwa(self, fa: File, fastq: File):  # input will be automatically converted if has type annotation
        """bwa mem -t {self.config.cpu} {fa} {fastq} | samtools view -Sb - > {fastq.stem}.bam"""
        return "*.bam"  # for ShellTask, str variable in the return will be treated as File and globed

    @shell
    def sort(bam: File):  # self is optional in case you don't want to access the current task
        """samtools sort -o {sorted_bam} {bam}"""
        sorted_bam = f"{bam.stem}.sorted.bam"
        return sorted_bam

    @shell(publish_dirs=["results/vcf"])
    def call(fa: File, bams: list):  # In case you need to write some python codes
        """samtools mpileup -g -f {fa} {bam_files} | bcftools call -mv - > all.vcf"""
        bam_files = ' '.join(str(bam) for bam in bams)
        return "all.vcf"

    @task
    def stats(vcf: File):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from pysam import VariantFile

        quals = [record.qual for record in VariantFile(str(vcf))]
        plt.hist(quals)

        plt.savefig("report.svg")

    @flow
    def call_vcf_flow():
        """Call vcf from fastq file.

        Parameters
        ----------
        fa: : str
            The path of genome file
        fastq: List[str]
            list of fastq files
        """

        def _call(bams):  # task is normal function, use python as wish
            return call(fa, bams)

        context = flowsaber.context
        fa = Channel.value(context.fa)
        fastq = Channel.values(*context.fastq)

        bam1 = bwa(fa, fastq)  # automatically clone channel
        bam2 = bwa(fa, fastq)
        mix(bam1, bam2) | sort | collect | _call | stats

    prefix = 'tests/test_flow/snamke-demo.nosync/data'
    with flowsaber.context({
        "fa": f'{prefix}/genome.fa',
        "fastq": [f'{prefix}/samples/{sample}' for sample in ['A.fastq', 'B.fastq', 'C.fastq']]
    }):
        # resolve dependency
        workflow = call_vcf_flow()
    run(workflow)


if __name__ == "__main__":
    test_snakemake_workflow()
    pass
