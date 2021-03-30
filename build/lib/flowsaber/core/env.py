import hashlib
import os
from pathlib import Path
from typing import Tuple

from dask.base import tokenize

from flowsaber.utility.logtool import get_logger

logger = get_logger(__name__)


class Env(object):
    def __init__(self, module: str = None, conda_path: str = None, image_file: str = None):
        self.module = module
        self.conda_path = conda_path
        self.image_file = image_file

    def __dask_tokenize__(self):
        return tokenize(self.__dict__)

    @property
    def rd1(self) -> str:
        return f" 1>>.__Env_stdout__.log "

    @property
    def rd2(self) -> str:
        return f" 2>>.__Env_stderr__.log "

    @staticmethod
    def exec_script_cmd(name: str, command: str, exec_cmd: str = "bash -e "):
        script_name = f".__Env__{name}__.sh"
        with open(script_name, 'w') as f:
            f.write(command)
        return f"{exec_cmd} {script_name} ;"

    def gen_cmd(self, cmd: str) -> str:
        assert cmd
        # load module
        load_module_cmd = ""
        if self.module:
            load_module_cmd = f"module purge {self.rd1} && module load {self.module} {self.rd1}"
        # activate conda
        activate_conda_cmd = ""
        if self.conda_path:
            assert Path(self.conda_path).is_dir()
            activate_conda_cmd = f"source activate {self.conda_path} {self.rd1}"

        cmd = f"{load_module_cmd} \n\n" \
              f"{activate_conda_cmd} \n\n" \
              f"{cmd}"

        # run with singularity image
        if self.image_file:
            assert Path(self.image_file).is_file()
            envs = '\n'.join(f"SINGULARITYENV_{k}=\"{v}\"" for k, v in os.environ.items())
            exec_cmd = f"{envs}\n" \
                       f"singularity " \
                       f"--quiet --silent " \
                       f"exec " \
                       f"--pwd `pwd` " \
                       f"--bind `pwd`:`pwd` " \
                       f"{self.image_file} " \
                       f"bash -e "
            cmd = self.exec_script_cmd('singularity_exec_cmd', cmd, exec_cmd)

        cmd = self.exec_script_cmd('bash_exec_cmd', cmd)

        return cmd


class EnvCreator(object):
    def __init__(self, module: str = None, conda: str = None, image: str = None):
        self.module = module
        self.conda = conda
        self.image = image
        self.hash = self.calculate_hash(self.module, self.conda, self.image)

    def __hash__(self):
        return hash(self.hash)

    @staticmethod
    def calculate_hash(module, conda, image) -> str:
        src = ""
        # module
        if module:
            src += module
        # conda
        if conda:
            if Path(conda).is_dir():
                src += str(Path(conda).expanduser().resolve())
            elif Path(conda).is_file():
                assert conda.endswith(('yml', 'yaml'))
                src += Path(conda).read_text()
            else:
                packages = conda.split()
                packages.sort()
                assert len(packages) > 0 and '/' not in conda
                src += str(packages)
        # image
        if image and Path(image).is_file():
            src += (Path(image).expanduser().resolve())

        md5hash = hashlib.md5()
        md5hash.update(src.encode())
        return md5hash.hexdigest()

    def gen_cmd_env(self) -> Tuple[str, Env]:
        create_conda_cmd = ""
        create_image_cmd = ""
        module = self.module
        conda_path = None
        image_file = None

        if self.conda:
            if Path(self.conda).is_dir():
                conda_path = Path(self.conda).expanduser().resolve()
            else:
                if Path(self.conda).is_file():
                    content = Path(self.conda).read_text()
                else:
                    packages = self.conda.split()
                    packages.sort()
                    dependencies = '\n'.join(f' - {p}' for p in packages)
                    content = "channels:\n" \
                              " - conda-forge\n" \
                              " - bioconda\n" \
                              " - defaults\n" \
                              "dependencies:\n" \
                              f"{dependencies}"
                env_file = Path(".__environment.yaml").resolve()
                env_file.write_text(content)
                conda_path = Path("conda_env").expanduser().resolve()
                create_conda_cmd = f"conda env create --force --quiet --file {env_file} --prefix {conda_path}"

        if self.image:
            if Path(self.image).is_file():
                image_file = Path(self.image).expanduser().resolve()
            else:
                image_file = Path("image.sig").resolve()
                create_image_cmd = f"singularity pull --name {image_file} {self.image}"

        cmd = f"\n\n{create_conda_cmd}\n\n" \
              f"\n\n{create_image_cmd}\n\n"

        return cmd, Env(module=module, conda_path=conda_path, image_file=image_file)
