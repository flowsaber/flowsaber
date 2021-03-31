from setuptools import setup, find_packages
import re

classifiers = [
    "Development Status :: 3 - Alpha",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Intended Audience :: Science/Research",
]

keywords = [
    "dataflow", "workflow", "asyncio"
]


def get_version():
    with open("flowsaber/__init__.py") as f:
        for line in f.readlines():
            m = re.match("__version__ = '([^']+)'", line)
            if m:
                return m.group(1)
        raise IOError("Version information can not found.")


def get_long_description():
    return "A dataflow based workflow framework."


def get_install_requires():
    requirements = []
    with open('requirements.txt') as f:
        for line in f:
            requirements.append(line.strip())
    return requirements


requirements = [
    'makefun',
    'graphviz',
    'dask',
    'cloudpickle',
    'ray',
    'rich'
]

setup(
    name='flowsaber',
    author='bakezq',
    author_email='zhongquan789@gmail.com',
    version=get_version(),
    license='GPLv3',
    description=get_long_description(),
    long_description=get_long_description(),
    keywords=keywords,
    url='https://github.com/zhqu1148980644/flowsaber',
    packages=find_packages(),
    # scripts=[],
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=requirements,
    extras_require={
      'dev': [
          'pytest',
          'pytest-cov',
          'matplotlib',
          'httpimport',
          'autodocsumm',
          'loky'
      ]
    },
    python_requires='>=3.6, <4',
)
