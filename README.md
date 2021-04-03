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

- Dataflow-like task composing syntax inspired from [nextflow](https://github.com/nextflow-io/nextflow) 's DSL2.
- Python based: Import/Compose/Modify Task/Flow objects at any time.
- DAG generation.
- Local Cache.
- Clustering support based on ray.
- Conda and Container execution environment.


### Install

```bash
pip install flowsaber
```

### Example

- A minimal working example consists most features and usages of `flowsaber`.

```python
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import flowsaber
from flowsaber import *

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

config.update({
    'cpu': 8,
    Task: {
        'executor': 'ray'
    }
})

# set input
num_ch = Channel.values(1, 2, 3, 4, 5, 6, 7, 8)
# resolve dependencies
workflow = my_flow(num=num_ch)
# now can generate dag
workflow.graph.render('quick_start_dag', view=False, format='png', cleanup=True)
# try run the flow
# if meet error, wrap code under if __name__ == '__main__':
asyncio.run(flowsaber.run(workflow))

# visualize the flow
img = mpimg.imread('quick_start_dag.png')
imgplot = plt.imshow(img)
plt.show(block=False)
```

### Test

```bash
python -m pytest tests -s -o log_cli=True -vvvv
```


### TODO
- [ ] Refactor DAG.
- [ ] Web interface.
- [ ] Pbs/Torque executor
- [ ] More cache mode.
- [ ] Supportrun in Cloud platform.
- [ ] Run CWL script, Convert between CWL and flowsaber script.

### Reference
- [nextflow](https://github.com/nextflow-io/nextflow)
- [prefect](https://github.com/PrefectHQ/prefect)
