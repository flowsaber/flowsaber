from setuptools import setup, find_packages
import re

classifiers = [
    "Development Status :: 3 - Alpha",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Intended Audience :: Science/Research",
]

keywords = [
    "dataflow"
]


def get_version():
    with open("pyflow/__init__.py") as f:
        for line in f.readlines():
            m = re.match("__version__ = '([^']+)'", line)
            if m:
                ver = m.group(1)
                return ver
        raise IOError("Version information can not found.")


def get_long_description():
    return "Dataflow based workflow framework"


def get_install_requires():
    requirements = []
    with open('requirements.txt') as f:
        for line in f:
            requirements.append(line.strip())
    return requirements


setup(
    name='pyflow',
    author='',
    author_email='',
    version=get_version(),
    license='GPLv3',
    description=get_long_description(),
    long_description=get_long_description(),
    keywords=keywords,
    # url='',
    packages=find_packages(),
    # scripts=[],
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=get_install_requires(),
    python_requires='>=3.8, <4',
)