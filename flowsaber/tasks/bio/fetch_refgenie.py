import json
from typing import List
from urllib import request

import flowsaber
from flowsaber.core.utility.target import File
from flowsaber.tasks.aws import S3LocalDownload


class FetchRefgenie(S3LocalDownload):
    ALIAS_DICT_API = 'http://refgenomes.databio.org/v3/genomes/alias_dict'
    DIR_CONTENTS_API = "http://refgenomes.databio.org/assets/dir_contents/{digest}/{assets}"
    S3_URL = "s3://awspds.refgenie.databio.org/refgenomes.databio.org/{digest}/{assets}__default/{fname}"

    default_config = {
        'workdir': "/tmp/flowsaber_fetch_refgenie"
    }

    def run(self, genome: str, assets: str) -> List[File]:
        file_urls = self.fetch_files(genome, assets)
        flowsaber.context.logger.info(f"Start downloading refgenie files: {file_urls} for "
                                      f"inputs: {genome=} {assets=} to: {self.context.get('run_workdir', '')}")
        return super().run(file_urls)

    def fetch_files(self, genome: str, assets: str):
        assert genome and assets
        with request.urlopen(self.ALIAS_DICT_API) as rsp:
            alias_dict: dict = json.loads(rsp.read().decode())
        _d = {}
        for digest, (genome, *_) in alias_dict.items():
            _d[genome] = digest
        alias_dict = _d
        if genome not in alias_dict:
            raise ValueError(f"{genome} should be one of {list(alias_dict.keys())}")
        digest = alias_dict[genome]

        with request.urlopen(self.DIR_CONTENTS_API.format(
                digest=digest, assets=assets
        )) as rsp:
            try:
                files: List[str] = json.loads(rsp.read().decode())
            except:
                raise ValueError(f"The assests: {assets} does not exists for genome: {genome}")

        return [self.S3_URL.format(digest=digest, assets=assets, fname=fname) for fname in files]


fetch_refgenie = FetchRefgenie()
