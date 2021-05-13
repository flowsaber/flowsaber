import re

from setuptools import setup, find_packages

classifiers = [
    "Development Status :: 3 - Alpha",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "License :: OSI Approved :: MIT License",
    "Intended Audience :: Science/Research",
    "Topic :: Scientific/Engineering :: Bio-Informatics",
    "Topic :: Software Development :: Libraries",
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
    with open("README.md") as f:
        readme = f.read()
    # remove html tag
    return re.sub("<.*>", '', readme)


def get_install_requires():
    requirements = []
    with open('requirements.txt') as f:
        for line in f:
            requirements.append(line.strip())
    return requirements


setup(
    name='flowsaber',
    author='bakezq',
    author_email='zhongquan789@gmail.com',
    version=get_version(),
    license='MIT',
    description="A dataflow based workflow framework",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    keywords=keywords,
    url='https://github.com/zhqu1148980644/flowsaber',
    packages=find_packages(),
    scripts=["scripts/flowsaber"],
    include_package_data=True,
    package_data={'': ['server/app/graphql_schema/*.graphql']},
    zip_safe=False,
    classifiers=classifiers,
    install_requires=[
        'makefun',
        'cloudpickle',
        'dask',
        'distributed',
        'psutil',
        'aiohttp',
        'pydantic',
        'fire',
        # server libs
        'ariadne',
        'uvicorn',
        'dnspython',
        'motor',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-cov',
            'httpimport',
            'autodocsumm',
            'pysam',
            'matplotlib'
        ],
        "bio": [
            "botocore"
        ]
    },
    python_requires='>=3.7, <4',
)
