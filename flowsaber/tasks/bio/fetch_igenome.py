import re
import threading
import urllib.request
from typing import List

import flowsaber
from flowsaber.core.utility.target import File
from flowsaber.tasks.aws import S3LocalDownload

FILE_LIST = None
FILE_LIST_LOCK = threading.Lock()


class FetchIgenome(S3LocalDownload):
    BUCKET_NAME = "ngi-igenomes"
    TYPE_SUFFIX = {
        "gtf": "Annotation/Genes/",
        "bed12": "Annotation/Genes/",
        "bismark": "Sequence/BismarkIndex/",
        "bowtie": "Sequence/BowtieIndex/",
        "bowtie2": "Sequence/Bowtie2Index/",
        "bwa": "Sequence/BWAIndex/",
        "star": "Sequence/STARIndex/",
        "fasta": "Sequence/WholeGenomeFasta/",
        "chromosomes": "Sequence/Chromosomes/",
        "abundantseq": "Sequence/AbundantSequences/",
        "smrna": "Annotation/SmallRNA/",
        "variation": "Annotation/Variation/",
        "gatk": "",
        "chromosizes": "Sequence/WholeGenomeFasta/GenomeSize.xml"
    }
    FILE_LIST_URL = "https://ewels.github.io/AWS-iGenomes/ngi-igenomes_file_manifest.txt"

    default_config = {
        'workdir': "/tmp/flowsaber_fetch_igenome"
    }

    def run(self, type: str, genome: str = None, source: str = None, build: str = None) -> List[File]:
        file_urls = self.fetch_files(type, genome, source, build)
        flowsaber.context.logger.info(f"Start downloading i-genome files: {file_urls} for "
                                      f"inputs: {genome=} {source=} {build=} {type=} to: {self.context['run_workdir']}")
        return super().run(file_urls)

    def fetch_files(self, type: str, genome: str, source: str, build) -> List[str]:
        """type is required, for example: bwa. genome, source, build should has at least one not-None value.
        """
        assert type and any((genome, source, build))
        # avoid re-download file list
        with FILE_LIST_LOCK:
            global FILE_LIST
            if not FILE_LIST:
                with urllib.request.urlopen(self.FILE_LIST_URL) as response:
                    path_list = response.read().decode().split("\n")
                FILE_LIST = path_list
            path_list = [p for p in FILE_LIST]

        # eagerly find the first matched file, and get valid genome, source, build
        valid_keys = [key for key in [genome, source, build] if key]
        path_list: List[str] = [path for path in path_list
                                if self.TYPE_SUFFIX[type] in path
                                and all(key in path for key in valid_keys)]
        genome, source, build = re.findall(
            r"s3://ngi-igenomes/igenomes/(.*?)/(.*?)/(.*?)/.*",
            (path_list or [])[0]
        )[0]

        # only download files in the first level
        pprefix = f"s3://{self.BUCKET_NAME}"
        prefix = f"{pprefix}/igenomes/{genome}/{source}/{build}/{self.TYPE_SUFFIX[type]}"
        fnames = [path
                  for path in path_list
                  if prefix in path and path.count('/') - prefix.count('/') == 0]
        assert fnames, f"No candidate file found with inputs: {type=} {genome=} {source=} {build=}"
        return fnames


fetch_igenome = FetchIgenome()
