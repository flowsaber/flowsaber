from flowsaber import ShellTask, File


class Bwa(ShellTask):
    """
    For xxx.fastq.gz return a file named xx.fastq.bam
    """
    DEFAULT_CONFIG = {
        'conda': "bwa==0.7.12 samtools==1.2"
    }

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


bwa = Bwa()
