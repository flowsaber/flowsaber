from flowsaber.core.task import File
from flowsaber.tasks.shell import ShellFlow


class Bwa(ShellFlow):
    """
    For xxx.fastq.gz return a file named xx.fastq.bam
    """

    def command(self, fa: File, fastq: File, params=None):
        """
        bwa mem {params_str} -t {self.cpu} {fa} {fastq} | samtools view -Sbh -o {bam}
        """
        if params:
            assert isinstance(params, (tuple, list, str))
            if isinstance(params, str):
                params = (params,)
        params_str = ' '.join(params) if params else ''
        bam = f"{fastq.stem}.bam"
        return bam


bwa = Bwa(conda="bwa==0.7.12 samtools==1.2")
