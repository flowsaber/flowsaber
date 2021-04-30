import hashlib
import os
import subprocess
from pathlib import Path
from typing import List, Union, Tuple, Optional, Any, TYPE_CHECKING

from dask.base import tokenize

from flowsaber.core.channel import Output
from flowsaber.core.flow import Flow
from flowsaber.core.task import Task
from flowsaber.core.utility.target import Stdin, Stdout, File
from flowsaber.core.utils import class_deco
from flowsaber.utility.utils import change_cwd, capture_local

if TYPE_CHECKING:
    from inspect import BoundArguments


class EnvBase(object):
    def __init__(self):
        self._cwd = None

    @property
    def cwd(self):
        assert self._cwd is not None
        return self._cwd

    @cwd.setter
    def cwd(self, path):
        self._cwd = path

    @property
    def cd(self):
        return f"mkdir -p {self.cwd}; cd {self.cwd}"

    @property
    def rd1(self) -> str:
        return f" 1>>{self.cwd + '/' if self.cwd else ''}.__Env_stdout__.log "

    @property
    def rd2(self) -> str:
        return f" 2>>{self.cwd + '/' if self.cwd else ''}.__Env_stderr__.log "


class EnvCreator(EnvBase):
    """Used for generating initialization script for enviroments.
    """

    def __init__(self, module: str = None, conda: str = None, image: str = None):
        super().__init__()
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

    def gen_cmd_env(self) -> Tuple[str, 'EnvRunner']:
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
                env_file = Path(f"{self.cwd}/.__environment.yaml").resolve()
                env_file.write_text(content)
                conda_path = Path(f"{self.cwd}/conda_env").expanduser().resolve()
                create_conda_cmd = f"\n\n" \
                                   f"{self.cd};" \
                                   f"conda env create --force --quiet --file {env_file} --prefix {conda_path};" \
                                   f"\n\n"

        if self.image:
            if Path(self.image).is_file():
                image_file = Path(self.image).expanduser().resolve()
            else:
                image_file = Path("image.sig").resolve()
                create_image_cmd = f"\n\n" \
                                   f"{self.cd};" \
                                   f"singularity pull --name {image_file} {self.image};" \
                                   f"\n\n"
        cmd = f"{create_conda_cmd}{create_image_cmd}"

        return cmd, EnvRunner(module=module, conda_path=conda_path, image_file=image_file)


class EnvRunner(EnvBase):
    """Generated by EnvCreator, used for generating commands that runs bash script in environments
    created by EnvCreator.
    """

    def __init__(self, module: str = None, conda_path: str = None, image_file: str = None):
        super().__init__()
        self.module = module
        self.conda_path = conda_path
        self.image_file = image_file

    def __dask_tokenize__(self):
        return tokenize(self.__dict__)

    def gen_cmd(self, cmd: str) -> str:
        assert cmd
        # load module
        load_module_cmd = ""
        if self.module:
            load_module_cmd = f"module purge {self.rd1} && module load {self.module} {self.rd1};\n\n"
        # activate conda
        activate_conda_cmd = ""
        if self.conda_path:
            assert Path(self.conda_path).is_dir()
            activate_conda_cmd = f"source activate {self.conda_path} {self.rd1}\n\n"

        cmd = f"{load_module_cmd}" \
              f"{activate_conda_cmd}" \
              f"{cmd}"

        # run with singularity image
        if self.image_file:
            assert Path(self.image_file).is_file()
            envs = dict(os.environ)
            envs.pop('PWD', None)
            envs = '\n'.join(f"SINGULARITYENV_{k}=\"{v}\"" for k, v in envs.items())
            escaped_cmd = cmd.replace("'", r"'\''")
            cmd = f"{envs}\n" \
                  f"singularity --quiet --silent " \
                  f"exec --pwd `pwd` --bind `pwd`:`pwd` " \
                  f"{self.image_file} " \
                  f"bash -c '{escaped_cmd}'"

        return cmd


class ShellTaskExecuteError(RuntimeError):
    pass


class CommandTaskComposeError(RuntimeError):
    pass


class ShellTask(Task):
    """Task that execute bash command by using subprocess.
    """

    default_config = {
        'publish_dirs': [],
    }

    def run(self, cmd: str, output=None):
        """This method should be thread safe, can not run functions depends one process-base attributes like ENV, ....

        Parameters
        ----------
        cmd
        output

        Returns
        -------

        """
        flow_workdir = self.context['flow_workdir']
        run_workdir = self.context['run_workdir']
        # run bash command in shell with Popen
        stdout_file, stderr_file = self.execute_shell_command(cmd, run_workdir)
        # 1. return stdout simulated by file
        if output is None:
            stdout_path = Path(stdout_file).resolve()
            stdout = Stdout(stdout_path)
            stdout.initialize_hash()
            return stdout

        # 2. return globed files
        collect_files: List[File] = []
        resolved_output = self.glob_output_files(output, run_workdir, collect_files)
        publish_dirs = self.get_publish_dirs(flow_workdir, self.config_dict.get('publish_dirs', []))
        for file in collect_files:
            file.initialize_hash()
            for pub_dir in publish_dirs:
                pub_dir.mkdir(parents=True, exist_ok=True)
                pub_file = pub_dir / Path(file.name)
                if not pub_file.exists():
                    file.link_to(pub_file)

        return resolved_output

    def execute_shell_command(self, cmd: str, run_workdir: Union[str, Path]) -> Tuple[Path, Path]:
        """Run command in shell

        Parameters
        ----------
        cmd
        run_workdir

        Returns
        -------

        """
        # this is not thread safe, because ENV is process-based attributes
        wrapped_cmd = f"mkdir -p {run_workdir};\n" \
                      f"cd {run_workdir};\n" \
                      f"{cmd}"
        # write into a sh file
        bash_script = Path(run_workdir, f".__run__.sh")
        with change_cwd(run_workdir):
            with bash_script.open('w') as f:
                f.write(wrapped_cmd)
        # execute with subprocess.Popen
        with change_cwd(run_workdir):
            envs = os.environ.copy()
            stdout_f = run_workdir / Path(f".__run__.stdout")
            stderr_f = run_workdir / Path(f".__run__.stderr")
            with stdout_f.open('w') as stdout_file, stderr_f.open('w+') as stderr_file:
                with subprocess.Popen(f"bash -e {bash_script};",
                                      stdout=stdout_file, stderr=stderr_file,
                                      env=envs, shell=True) as p:
                    p.wait()

                stderr_file.seek(0)
                stderr = stderr_file.read(1000)
                if p.returncode:
                    raise ShellTaskExecuteError(f"Execute bash file: {bash_script} meets "
                                                f"error: {stderr} in {run_workdir}.")

            return stdout_f, stderr_f

    @classmethod
    def glob_output_files(cls, item, run_workdir, collect_files: List[File]):
        """Iterate over the item hierarchically and convert in-place and glob found str into Files
        and collect them into the third parameter.

        Parameters
        ----------
        item
        run_workdir
        collect_files

        Returns
        -------

        """
        if type(item) is str:
            # key step
            files = [File(p.resolve())
                     for p in Path(run_workdir).glob(item)
                     if not p.name.startswith('.') and p.is_file()]
            cls.glob_output_files(files, run_workdir, collect_files)
            # may globed multiple files
            return files[0] if len(files) == 1 else tuple(files)

        elif isinstance(item, dict):
            for k, v in item.items():
                item[k] = cls.glob_output_files(v, run_workdir, collect_files)
        elif isinstance(item, (tuple, list)):
            if isinstance(item, tuple):
                item = list(item)
            for i, v in enumerate(item):
                item[i] = cls.glob_output_files(v, run_workdir, collect_files)
        elif isinstance(item, File):
            # collect File
            collect_files.append(item)

        return item

    @staticmethod
    def get_publish_dirs(flow_workdir, configured_publish_dirs: List[str]):
        """Get absolute path of configured publish dirs.

        Parameters
        ----------
        flow_workdir
        configured_publish_dirs

        Returns
        -------

        """
        # resolve publish dirs
        publish_dirs = []
        for pub_dir in configured_publish_dirs:
            pub_dir = Path(pub_dir).expanduser().resolve()
            if pub_dir.is_absolute():
                publish_dirs.append(pub_dir)
            else:
                publish_dirs.append(Path(flow_workdir, pub_dir).resolve())
        return publish_dirs


class CommandTask(ShellTask):
    """Task used for composing bash command based on outputs data of channels. Users need to implement the
    command method.
    Note that the _output Channel of this task simply emits composed bash command in str type, and this
    bash command needs to be actually executed by ShellTask.
    """
    FUNC_PAIRS = [('command', 'run')]

    default_config = {
        'module': None,
        'conda': None,
        'image': None,
        'env_path': None,
    }

    def __init__(self, **kwargs):
        super().__init__(num_out=2, **kwargs)
        self.env_creator: Optional[EnvCreator] = None

    @property
    def task_hash(self) -> str:
        """take command's source code into consideration

        Returns
        -------

        """
        run_hash = super().task_hash
        fn = self.command
        # for wrapped func, use __source_func__ as a link
        while hasattr(fn, '__source_func__'):
            fn = fn.__source_func__
        code = fn.__code__.co_code
        annotations = getattr(fn, '__annotations__', None)

        return tokenize(code, annotations, run_hash)

    @property
    def env(self) -> EnvCreator:
        if self.env_creator is None:
            module = self.config_dict.get('module', None)
            conda = self.config_dict.get('conda', None)
            image = self.config_dict.get('image', None)
            self.env_creator = EnvCreator(module, conda, image)

        return self.env_creator

    @property
    def input_hash_source(self) -> dict:
        return {
            'env_info': self.env.hash
        }

    def run_hash_source(self, run_data: 'BoundArguments', **kwargs) -> dict:
        return {
            'env_hash': self.env.hash
        }

    def command(self, *args, **kwargs) -> str:
        """Users need to implement this function to compose the final bash command.

        The returned value of this method represents the expected outputs after executing the
        composed bash command in shell:
            1. None represents the output is stdout.
            2. str variables represents glob syntax for files in the working directory.

        To tell flowsaber what's the composed bash command, users has two options:
            1: Assign the composed command to a variable named CMD.
            2: Write virtual fstring as the docstring of command method. All variables in the command method
                scoped can be used freely.

        Here are some examples:

            class A(CommandTask):
                def command(self, fa, fasta):
                    "bwa map -t {self.context.cpu} {fa} {fasta} -o {bam_file}"
                    bam_file = "test.bam"
                    return bam_file

            class B(CommandTask):
                def command(self, file):
                    a = "xxxx"
                    b = 'xxxx'
                    CMD = f"echo {a}\n"
                          f"echo {b}\n"
                          f"cat {file}"
                    # here implicitly returned a None, represents the _output of cmd is stdout

        Parameters
        ----------
        args
        kwargs
        """
        raise NotImplementedError("Please implement this function and return a bash script.")

    def resolve_command(self, *args, **kwargs) -> Tuple[str, Any]:
        # TODO this cause error in debug mode, since settrace is forbidden in debug mode.
        # TWO options: 1: use local var CMD or use docstring
        with capture_local() as local_vars:
            cmd_output = self.command(*args, **kwargs)
        cmd = local_vars.get("CMD", None)
        if cmd is None:
            cmd = self.command.__doc__
            if cmd is None:
                raise CommandTaskComposeError("CommandTask must be registered with a shell commands by "
                                              "assigning the commands into a CMD variable inside the function or "
                                              "adding the commands as the command() method's documentation by "
                                              "setting `__doc__`.")
            local_vars.update({'self': self})
            cmd = cmd.format(**local_vars)
        # add cat stdin cmd
        stdins = [arg for arg in list(args) + list(kwargs.values()) if isinstance(arg, Stdin)]
        if len(stdins) > 1:
            raise CommandTaskComposeError(f"Found more than two stdin inputs: {stdins}")
        stdin = f"{stdins[0]} " if len(stdins) else ""
        cmd = f"{stdin}{cmd}"

        return cmd, cmd_output

    def run(self, *args, **kwargs):
        run_workdir = self.context['run_workdir']
        env_path = self.config_dict['env_path'] or self.context['run_workdir']

        # create the environment
        env_creator = self.env
        env_creator.cwd = env_path
        with change_cwd(env_path):
            self.context['run_workdir'] = env_path
            create_env_cmd, env_runner = env_creator.gen_cmd_env()
            self.context['run_workdir'] = run_workdir

        if create_env_cmd:
            super().run(create_env_cmd)

        # wrap the command with env
        with change_cwd(run_workdir):
            cmd, cmd_output = self.resolve_command(*args, **kwargs)
            env_runner.cwd = ""
            env_wrapped_cmd = env_runner.gen_cmd(cmd)

        return env_wrapped_cmd, cmd_output


class ShellFlow(Flow):
    FUNC_PAIRS = [('command', 'run')]

    def command(self, *args, **kwargs):
        raise NotImplementedError("Users need to implement this method to indicate flowsaber how to "
                                  "compose the bash command.")

    def run(self, *args, **kwargs) -> Output:
        command_task = command(type(self).command, **self.rest_kwargs)
        shell_task = ShellTask(**self.rest_kwargs)
        cmd_ch, cmd_output_ch = command_task(*args, **kwargs)
        output_ch = shell_task(cmd_ch, cmd_output_ch)

        return output_ch


command = class_deco(CommandTask, 'command')
shell = class_deco(ShellFlow, 'command')