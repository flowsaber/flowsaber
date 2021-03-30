# Executor


ShellTask support several options to enable the bash script run within specific environments.

### Conda

- `conda="bwa samtools"`
- `conda="env.yaml"`
- `conda="path/to/environment"`


### Singularity

- `image="docker://bioconda"`


### Example

```python
@shell(conda='bwa', image='docker://ubuntu')
def bwa_map(fa, fastq):
    ...
```