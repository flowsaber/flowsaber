import re
import shutil
import tempfile
import threading
import urllib.request
from pathlib import Path
from typing import List

from botocore import UNSIGNED
from botocore.config import Config

import flowsaber
from flowsaber.core.task import Task
from flowsaber.core.utility.target import File
from flowsaber.utility.aws import get_boto_client

FILE_LIST = None
FILE_LIST_LOCK = threading.Lock()


class FetchIgenome(Task):
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
        "gatk": ""
    }
    FILE_LIST_URL = "https://ewels.github.io/AWS-iGenomes/ngi-igenomes_file_manifest.txt"
    DEFAULT_BOTO_KWARGS = {
        'config': Config(signature_version=UNSIGNED)
    }

    default_config = {
        'workdir': "/tmp/flowsaber_fetch_igenome"
    }

    def __init__(self, boto_kwargs: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.boto_kwargs = boto_kwargs or self.DEFAULT_BOTO_KWARGS

    def inspect_files(self, type: str, genome: str, source: str, build) -> List[str]:
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
        fnames = [path[len(pprefix) + 1:]
                  for path in path_list
                  if prefix in path and path.count('/') - prefix.count('/') == 0]
        assert fnames, f"No candidate file found with inputs: {type=} {genome=} {source=} {build=}"
        return fnames

    def run(self, type: str, genome: str = None, source: str = None, build: str = None) -> List[File]:
        run_workdir = self.context['run_workdir']
        fnames = self.inspect_files(type, genome, source, build)

        s3 = get_boto_client("s3", **self.boto_kwargs)

        tmp_files = []
        local_files = []
        flowsaber.context.logger.info("Start downloading")
        # for each file, download into a temporary directory, then move it the run_workdir
        try:
            flowsaber.context.logger.info(f"Start downloading {fnames} for "
                                          f"inputs: {genome=} {source=} {build=} {type=}")
            for fname in fnames:
                flowsaber.context.logger.debug(f"Start downloading {fname}.")
                # download into temp file
                fd, path = tempfile.mkstemp()
                tmp_files.append(path)
                with open(fd, 'wb') as fstream:
                    s3.download_fileobj(Bucket=self.BUCKET_NAME, Key=fname, Fileobj=fstream)
                # move to run_workdir
                local_file = Path(run_workdir, fname)
                local_file.parent.mkdir(parents=True, exist_ok=True)  # mkdir
                local_file.touch()  # touch
                shutil.copy(path, local_file)  # move
                local_files.append(File(local_file))
                flowsaber.context.logger.debug(f"Done downloading {fname}, moved to {local_file}.")
        finally:
            for path in tmp_files:
                Path(path).unlink(missing_ok=True)  # rm
        # sort by filename length
        return sorted(local_files, key=lambda x: len(str(x)))


fetch_igenome = FetchIgenome()
