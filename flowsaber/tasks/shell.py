import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Union, Tuple, Any

import flowsaber
from flowsaber.core.task import Task
from flowsaber.core.utility.target import Stdin, Stdout, File
from flowsaber.core.utils import class_deco
from flowsaber.utility.utils import change_cwd, capture_local


class ShellTaskExecuteError(RuntimeError):
    pass


class CommandTaskComposeError(RuntimeError):
    pass


class BashTask(Task):
    """Task that execute bash command by using subprocess.
    """

    default_config = {
        'publish_dirs': [],
    }

    def run(self, cmd: str, output=None, envs: dict = None):
        """This method should be thread safe, can not run functions depends one process-base attributes like ENV, ....

        Parameters
        ----------
        cmd
        output
        envs: dict

        Returns
        -------

        """
        flow_workdir = self.context.get('flow_workdir', '')
        run_workdir = self.context.get('run_workdir', '')
        # important! make sure run is atomic
        if Path(run_workdir).is_dir():
            shutil.rmtree(run_workdir, ignore_errors=True)
        # 1. run bash command in shell with Popen
        stdout_file, stderr_file = self.execute_command(cmd, run_workdir, envs)

        # 2. handle output
        #   1. return stdout simulated by file
        if output is None:
            stdout_path = Path(stdout_file).resolve()
            stdout = Stdout(stdout_path)
            stdout.initialize_hash()
            return stdout

        #   2. return globed files
        collect_files: List[File] = []
        resolved_output = self.glob_output_files(output, run_workdir, collect_files)
        publish_dirs = self.get_publish_dirs(flow_workdir, self.config_dict.get('publish_dirs', []))
        # TODO use bash script instead of python codes
        for file in collect_files:
            file.initialize_hash()
            for pub_dir in publish_dirs:
                pub_dir.mkdir(parents=True, exist_ok=True)
                pub_file = pub_dir / Path(file.name)
                if not pub_file.exists():
                    file.link_to(pub_file)
        return resolved_output

    def execute_command(self, cmd: str, run_workdir: Union[str, Path], envs: dict = None) -> Tuple[Path, Path]:
        """Run command in shell

        Parameters
        ----------
        cmd
        run_workdir
        envs

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
        flowsaber.context.logger.debug(f"Executed shell cmd: \n{wrapped_cmd}\n in path: \n{run_workdir}\n")
        # execute with subprocess.Popen
        with change_cwd(run_workdir):
            envs = envs or os.environ.copy()
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


class ShellTask(BashTask):
    """Task used for composing bash command and then execute the command. Users need to implement the
    command method.
    Note that the _output Channel of this task simply emits composed bash command in str type, and this
    bash command needs to be actually executed by ShellTask.
    """
    FUNC_PAIRS = [('command', 'run')]

    @property
    def task_hash(self) -> str:
        # TODO add function hash of command
        return super().task_hash

    def run(self, *args, **kwargs):
        cmd, cmd_output = self.compose_command(*args, **kwargs)
        return super().run(cmd, cmd_output)

    def compose_command(self, *args, **kwargs) -> Tuple[str, Any]:
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
            # TODO too dangerous
            # use `cmd.format` is not enough for all cases
            cmd = eval(f"f\"\"\"{cmd}\"\"\"", local_vars)

        # add cat stdin cmd
        stdins = [arg for arg in list(args) + list(kwargs.values()) if isinstance(arg, Stdin)]
        if len(stdins) > 1:
            raise CommandTaskComposeError(f"Found more than two stdin inputs: {stdins}")
        stdin = f"{stdins[0]} " if len(stdins) else ""
        cmd = f"{stdin}{cmd}"

        return cmd, cmd_output

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


bash = BashTask()
shell = class_deco(ShellTask, 'command')
