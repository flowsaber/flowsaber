import hashlib
import os
import shutil
import uuid
from pathlib import Path
from typing import Union, Tuple

from pyflow.utility.logtool import get_logger

logger = get_logger(__name__)


class EnvBase(object):
    ENV_FOLDER: str = "EnvBase"

    def __init__(self, workdir: Union[str, Path], **kwargs):
        self.workdir = Path(workdir).expanduser().resolve()
        self._env_path = None
        self._script_name = None

    @property
    def hash_source(self) -> str:
        return ""

    @property
    def hash(self) -> str:
        src = self.hash_source or uuid.uuid4().hex
        md5hash = hashlib.md5()
        md5hash.update(src.encode())
        return md5hash.hexdigest()

    @property
    def env_path(self) -> Path:
        # create workdir/content_hash
        if self._env_path is None:
            self._env_path = Path(self.workdir, 'Environments', self.name, self.hash).expanduser().resolve()
            self._env_path.mkdir(parents=True, exist_ok=True)
        return self._env_path

    def clean_env_path(self):
        env_path = self.env_path
        if env_path.is_dir():
            try:
                shutil.rmtree(env_path)
            except Exception as e:
                logger.warning(f"Cleaning up {env_path} failed with error: {e}")

    def init(self):
        pwd = os.getcwd()
        assert pwd != str(self.workdir), "Pwd is the same as workdir, may you should " \
                                         "cd into a Task's path before init env."
        files = list(Path().glob(".__*"))
        for f in files:
            f.unlink()

    @property
    def rd1(self) -> str:
        # TODO find a time point to clear
        return f" 1>>.__{self.name}_stdout__.log "

    @property
    def rd2(self) -> str:
        return f" 2>>.__{self.name}_stderr__.log"

    def prepare_cmd(self, command: str) -> Tuple[str, str]:
        return command, ''

    def run_cmd(self, command: str) -> Tuple[str, str]:
        """should redirect stdout to elsewhere, make sure only the stdout of true command will be captured.
        """
        return command, ''

    @property
    def name(self):
        return type(self).__name__

    def script(self, name):
        return f".__{self.name}__{name}__.sh"

    def exec_script_cmd(self, script_name: str, command: str, exec_cmd: str = "bash -e "):
        self._script_name = script_name
        with open(script_name, 'w') as f:
            f.write(command)
        return f"{exec_cmd} {script_name} ;"

    def __dask_tokenize__(self):
        return self.hash


class Module(EnvBase):

    def __init__(self, modules: str = "", **kwargs):
        super().__init__(**kwargs)
        self.modules = modules

    @property
    def hash_source(self) -> str:
        return self.modules

    def run_cmd(self, command) -> Tuple[str, str]:
        append = f"module purge {self.rd1} && module load {self.modules} {self.rd1}"
        return f"\n\n{append}\n\n{command}", ''


class Conda(EnvBase):

    def __init__(self, conda_env: str = "", **kwargs):
        super().__init__(**kwargs)
        self.conda_env, self.content = self.check_env(conda_env)
        if self.content:
            self.env_file = self.env_path / Path(f"environment.yaml")
        else:
            self.env_file = None
            self.__dict__['env_path'] = Path(conda_env).expanduser().resolve()

    @property
    def hash_source(self) -> str:
        return self.content or self.conda_env

    @staticmethod
    def check_env(env: str):
        if Path(env).is_dir():
            return str(Path(env).expanduser().resolve()), None
        elif env.endswith(('yml', 'yaml')):
            assert Path(env).is_file(), f"The input file {env} does not exists."
            content = Path(env).read_text()
            assert len(content) > 0
        else:
            assert '/' not in env
            packages = env.split()
            packages.sort()
            assert len(packages) > 0
            dependencies = '\n'.join(f' - {p}' for p in packages)
            content = "channels:\n" \
                      " - conda-forge\n" \
                      " - bioconda\n" \
                      " - defaults\n" \
                      "dependencies:\n" \
                      f"{dependencies}"
        return env, content

    def prepare_cmd(self, command):
        append = ""
        if self.env_file:
            self.env_file.write_text(self.content)
            # create conda_env only if conda_env does not exist
            append = f"if ! conda info --envs | grep {self.env_path}; then\n" \
                     f"    conda env create --force --quiet " \
                     f"    --file {self.env_file} " \
                     f"    --prefix {self.env_path}; \n" \
                     f"fi;"
        return f"\n\n{append}\n\n{command}", ''

    def run_cmd(self, command) -> Tuple[str, str]:
        append = f"source activate {self.env_path} {self.rd1}"
        return f"\n\n{append}\n\n{command}", ''


class Container(EnvBase):
    def __init__(self, image: str = "", **kwargs):
        super().__init__(**kwargs)
        if Path(image).is_file():
            image = str(Path(image).expanduser().resolve())
        assert image
        self.image = image

    @property
    def hash_source(self) -> str:
        return self.image


class Singularity(Container):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        from sys import platform
        if platform not in ('linux', 'linux2', 'win32'):
            raise RuntimeError(f"Currently {platform} does not support singularity.")

        if Path(self.image).is_file():
            self.img_file = Path(self.image)
        else:
            self.img_file = Path(self.env_path) / Path("image.sig")

    def prepare_cmd(self, command: str) -> Tuple[str, str]:
        # pull image only if img_file does not exist
        # TODO version 3.4 support --dir option, then remove mv command
        img_name = self.img_file.name
        append = f"if [ ! -f {self.img_file} ]; then\n" \
                 f"    singularity pull --name {img_name} {self.image}; \n" \
                 f"    mv {img_name} {self.img_file.parent}; \n" \
                 f"fi;"
        return f"\n\n{append}\n\n{command}", ''

    def run_cmd(self, command: str) -> Tuple[str, str]:
        envs = '\n'.join(f"SINGULARITYENV_{k}=\"{v}\"" for k, v in os.environ.items())
        exec_cmd = f"{envs}\n" \
                   f"singularity " \
                   f"--quiet --silent " \
                   f"exec " \
                   f"--pwd `pwd` " \
                   f"--bind {self.workdir}:{self.workdir} " \
                   f"{self.img_file} " \
                   f"bash -e "
        script = self.script('run')
        return self.exec_script_cmd(script, command, exec_cmd), script


class Docker(EnvBase):
    pass


class Env(EnvBase):

    def __init__(self,
                 modules: str = None,
                 conda_env: str = None,
                 image: str = None, runtime: str = "singularity", **kwargs):
        super().__init__(**kwargs)
        envs = []
        if modules:
            envs.append(Module(modules=modules, **kwargs))
        if conda_env:
            envs.append(Conda(conda_env=conda_env, **kwargs))
        if image:
            if runtime == 'singularity':
                envs.append(Singularity(image=image, **kwargs))
            else:
                raise ValueError(f"{runtime} not supported now.")
        self.envs = envs

    def prepare_cmd(self, command: str) -> Tuple[str, str]:
        for env in self.envs:
            command, _ = env.prepare_cmd(command)
        script = self.script('prepare')
        return self.exec_script_cmd(script, command), script

    def run_cmd(self, command: str) -> Tuple[str, str]:
        for env in self.envs:
            command, _ = env.run_cmd(command)
        script = self.script('run')
        return self.exec_script_cmd(script, command), script

    def hash_source(self) -> str:
        return "-".join(env.hash for env in self.envs)
