# A dataflow based workflow framework.
### work in progress

<p align="center">

  

  
  <a href="https://pypi.python.org/pypi/flowsaber/">
    <img src="https://img.shields.io/pypi/v/flowsaber.svg" alt="Install with PyPi" />
  </a>
  
  
  <a href="https://github.com/zhqu1148980644/flowsaber/releases">
  	<img src="https://img.shields.io/github/v/release/zhqu1148980644/flowsaber?include_prereleases&label=github" alt="Github release">
  </a>
 
  <a href="https://zhqu1148980644.github.io/flowsaber/index.html">
  	<img src="https://readthedocs.org/projects/ansicolortags/badge/?version=latest" alt="Documentation">
  </a>
  
  <a href="https://pypi.python.org/pypi/flowsaber">
    <img src="https://img.shields.io/pypi/pyversions/flowsaber.svg" alt="Version">
  </a>
  
  <a href="https://pepy.tech/project/flowsaber">
    <img src="https://pepy.tech/badge/flowsaber" alt="Downloads">
  </a>

  <a href="https://pepy.tech/project/flowsaber">
    <img src="https://pepy.tech/badge/flowsaber/week" alt="Downloads per week">
  </a>
  
  <a href="https://github.com/zhqu1148980644/flowsaber/actions/workflows/python-package-conda.yml">
    <img src="https://github.com/zhqu1148980644/flowsaber/actions/workflows/python-package-conda.yml/badge.svg" alt="Build Status">
  </a>

  <a href="https://app.codecov.io/gh/zhqu1148980644/flowsaber">
    <img src="https://codecov.io/gh/zhqu1148980644/flowsaber/branch/main/graph/badge.svg" alt="codecov">
  </a>

  <a href="https://github.com/zhqu1148980644/flowsaber/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/zhqu1148980644/flowsaber" alt="license">
  </a>

</p>




### Features

- Dataflow-like flow/task composing syntax inspired from [nextflow](https://github.com/nextflow-io/nextflow) 's DSL2.
- Pure python: No DSL, Import/Compose/Modify Task/Flow python objects at will.
    - Extensible and interactive due to dynamic nature of Python.
        - Task Cache.
        - Conda and Container execution environment.
        - ...
- Distributable: Use Dask distributed as Task executor, can deploy in local, cluster, cloud.
- Support hybrid model inspired from [prefect](https://github.com/PrefectHQ/prefect).
    - Build Flow in Local python or web UI(In progress).
    - Schedule/Monitor flow execution in remote server through python or web UI(In progress).

### Install

```bash
pip install flowsaber
```

### Example

- A minimal working example consists most features and usages of `flowsaber`.

```python
from flowsaber.api import *

@task
def add(self, num):  # self is optional
    return num + 1

@task
def multiply(num1, num2):
    return num1 * num2

@shell
def write(num):
    """echo {num} > 1.txt"""
    return '*.txt'

@task
def read(f: File):
    return open(str(f)).readlines()

@flow
def sub_flow(num):
    return add(num) | map_(lambda x: x ** 2) | add

@flow
def my_flow(num):
    [sub_flow(num), sub_flow(num)] | multiply \
    | write | read | flatten \
    | map_(lambda x: int(x.strip())) \
    | view

num_ch = Channel.values(1, 2, 3, 4, 5, 6, 7, 8)
# resolve dependencies
workflow = my_flow(num=num_ch)
run(workflow)
```


### Example to run in remote

##### Start server(API endpoint)
In bash shell.
```bash
flowsaber server
```

##### Start agent(Flow dispatcher)
In bash shell.
```bash
flowsaber agent --server "http://127.0.0.1:8000" --id test
```

##### Create flow and schedule for running
In python script or IPython console.
```python
from flowsaber.api import *
@task
def add(num):
    print("This is meesage send by print to stdout in task")
    print("This is meesage send by print to stderr in task", file= sys.stderr)
    a = 1
    for i in range(10000000):
        a += 1
    return num + 1

@flow
def myflow(num):
    return num | add | add | view | add | view

num_ch = Channel.values(*list(range(10)))
f = myflow(num_ch)

run(f, server_address="http://127.0.0.1:8000", agent_id="test")
```

### Test

```bash
python -m pytest tests -s -o log_cli=True -vvvv
```


### TODO

- [ ] Web interface.
- [ ] Pbs/Torque executor
- [ ] More cache mode.
- [ ] Supportrun in Cloud platform.
- [ ] Run CWL script, Convert between CWL and flowsaber flow.

### Reference
- [nextflow](https://github.com/nextflow-io/nextflow)
- [prefect](https://github.com/PrefectHQ/prefect)
