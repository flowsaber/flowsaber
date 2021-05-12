# comes from prefect
import io

from flowsaber.core.task import Task
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
