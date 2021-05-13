# parts are comes from prefect
import io
import shutil
import tempfile
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import List, Tuple
from urllib import parse

from botocore import UNSIGNED
from botocore.config import Config

import flowsaber
from flowsaber.core.task import Task
from flowsaber.core.utility.target import File
from flowsaber.utility.aws import get_boto_client


class S3Download(Task):
    def __init__(self, boto_kwargs: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.boto_kwargs = boto_kwargs or {}

    def run(self, bucket: str, key: str, credentials: str = None) -> bytes:
        """
        Task run method.
        Args:
            - bucket (str): the name of the S3 Bucket to download from
            - key (str): the name of the Key within this bucket to retrieve
            - credentials (dict, optional): your AWS credentials passed from an upstream
                Secret task; this Secret must be a JSON string
                with two keys: `ACCESS_KEY` and `SECRET_ACCESS_KEY` which will be
                passed directly to `boto3`.  If not provided here or in context, `boto3`
                will fall back on standard AWS rules for authentication.
            - compression (str, optional): specifies a file format for decompression, decompressing
                data upon download. Currently supports `'gzip'`.
            - as_bytes (bool, optional): If true, result will be returned as
                `bytes` instead of `str`. Defaults to False.
        Returns:
            - str: the contents of this Key / Bucket, as a string or bytes
        """
        s3_client = get_boto_client("s3", credentials=credentials, **self.boto_kwargs)
        stream = io.BytesIO()

        # download
        s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)

        # prepare data and return
        stream.seek(0)
        return stream.read()


class S3LocalDownload(Task):
    DEFAULT_BOTO_KWARGS = {
        'config': Config(signature_version=UNSIGNED)
    }

    default_config = {
        'workdir': "/tmp/flowsaber_aws_s3_download"
    }

    def __init__(self, num_thread: int = 4, boto_kwargs: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.num_thread = num_thread
        self.boto_kwargs = boto_kwargs or self.DEFAULT_BOTO_KWARGS

    def run(self, file_urls: List[str]):
        run_workdir = self.context.get('run_workdir', "")

        local_files = []

        def done_callback(fut: Future):
            nonlocal local_files
            new_local_files = fut.result()
            flowsaber.context.logger.debug(f"Done downloading: {new_local_files}")
            local_files += new_local_files

        futs = []
        with ThreadPoolExecutor(self.num_thread) as pool:
            for file_url in file_urls:
                fut = pool.submit(self.download_and_check, file_url, run_workdir)
                fut.add_done_callback(done_callback)
                futs.append(fut)

        # sort by filename length
        return sorted(local_files, key=lambda x: len(str(x)))

    def download_and_check(self, file_url, run_workdir):
        s3 = get_boto_client("s3", **self.boto_kwargs)
        # for each file, download into a temporary directory, then move it the run_workdir
        # download into temp file
        fd, path = tempfile.mkstemp()
        local_files = []
        try:
            bucket, key = self.resolve_url(file_url)
            with open(fd, 'wb') as fstream:
                s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=fstream)
            # move to run_workdir
            local_file = Path(run_workdir, bucket, key).resolve()
            local_file.parent.mkdir(parents=True, exist_ok=True)  # mkdir
            local_file.touch()  # touch
            shutil.copy(path, local_file)  # move
            local_files.append(File(local_file))
        finally:
            Path(path).unlink(missing_ok=True)  # rm

        return local_files

    @staticmethod
    def resolve_url(url: str) -> Tuple[str, str]:
        o = parse.urlparse(url, allow_fragments=False)
        return o.netloc, o.path.strip("/")
