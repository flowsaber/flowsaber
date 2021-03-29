import re
from functools import partial
from typing import Tuple
from dataclasses import dataclass
from pyflow import *


def iter_list(li, item, res):
    if isinstance(item, dict):
        for k, v in item.items():
            li.append(k)
            iter_list(li, v, res)
            li.pop()
    elif isinstance(item, (tuple, list)):
        for v in item:
            iter_list(li, v, res)
    else:
        res.append(li + [item])


def cat(file):
    if str(file).endswith('.gz'):
        return f" gzip -c -d -n {file} "
    else:
        return f" cat {file} "


def is_fastq(file):
    return str(file).endswith(('.fastq.gz', '.fastq', '.fq', '.fq.gz'))


def is_sra(file):
    return not is_fastq(file) and (str(file).endswith('.sra') or str(file).startswith(('SRR', 'SRX')))


def is_url(file):
    return isinstance(file, str) and file.startswith(('http:', 'https:', 'ftp:'))


@shell
def fetch(*args, **kwargs):
    return None


@dataclass
class Run:
    group: str
    url: Union[str, Path]

    def __call__(self, **kwargs):
        self.__dict__.update(kwargs)


@map_
def check_files(run: Run):
    url = run.url
    if Path(url).expanduser().is_file():
        return run(url=Path(url))

    files = [p.resolve() for p in list(Path().glob(url)) if p.is_file()]
    if len(files):
        return [run(url=f) for f in files]
    files = [p.resolve() for p in list(Path('/').glob(url)) if p.is_file()]
    if len(files):
        return [run(url=f) for f in files]
    return is_url(url)


@shell
def download(run: Run):
    if Path(run.url).is_file():
        return run
    elif is_sra(run.url):
        Shell(f"prefetch {run.url}")
    else:
        Shell(f"axel -n 30 {run.url}")
    return run(file=File('.*'))


@download.skip
def is_file(run: Run):
    return Path(run.url).is_file()


@shell
def fastq_dump(self, run: Run):
    Shell(f"parallel-fastq-dump --sra-id {run.url} --threads {self.thread} --gzip --split-files")
    return run(url=File('*_1*.fq.gz')), run(url=File("*_1*.fq.gz"))


@fastq_dump.skip
def not_sra(run: Run):
    return not is_sra(run.url)


@shell
def digest(fasta: File, chromsizes: File, enzyme: str):
    frag = f"{fasta.stem}.{enzyme}.bed"

    Shell(f"cooler digest --out ${frag} ${chromsizes} ${fasta} ${enzyme}")

    return frag


@shell
def map_parse_sort(self, index: File, chromsizes: File, fastq_pair: Tuple[File, File]):
    fq1, fq2 = fastq_pair
    name = fq1.name
    bam = f"{name}.bam"
    pair = f"{name}.pair.gz"
    keep_bam = f"| tee > (samtools sort -O BAM > {bam})" if self.merge_bam else ""
    Shell(f"touch {bam}\n"
          f"bwa mem -SP5M -t {self.thread} {index} {fq1} {fq2} {keep_bam}"
          f"    | pairtools parse --assembly {self.assembly} -c {chromsizes} {self.parse_args}"
          f"    | pairtools sort --nproc {self.thread} --tmpdir {self.sort_tmpdir} -o {pair}")

    return bam, pair


@shell(
    publish="results/fastqc"
)
def fastqc(self, fastq_pair: File):
    fq1, fq2 = fastq_pair
    Shell(f"fastqc --threads {self.thread} -o `pwd` {fq1} {fq2}")
    return "*"


@shell(
    publish="results/multiqc"
)
def mutliqc(fastqc_dir):
    Shell(f"multiqc {fastqc_dir}")
    return "*"


@shell(
    storedir="results/pairs/exp",
    cpus=10,
    memory="10 GB"
)
def select_pair(self, pair: File, frag: File):
    name = pair.stem
    rest_pair = f"{name}.rest.pair.gz"
    filtered_pair = f"{name}.filtered.pair.gz"

    restrict_cmd = f" | pairtools restrict -f {frag} " if self.do_restrict else ""
    if self.select_expr:
        select_cmd = f" | pairtools select --output-rest {rest_pair} -o {filtered_pair} \"{self.select_expr}\""
    else:
        select_cmd = f"-o {filtered_pair} \n touch {rest_pair}"

    Shell(f"{cat(pair)} {restrict_cmd} {select_cmd}")

    return filtered_pair, rest_pair


@shell(
    storedir="results/pairs/bio",
    cpus=10,
    memory="5 GB"
)
def merge_dedup_pair(self, group_pairs):
    group, pairs = group_pairs
    pair = f"{group}.dedup.pair.gz"
    duplicate = f"{group}.dups.pair.gz"

    Shell(f"pairtoosl merge --nproc {self.thread} {' '.join(pairs)} "
          f"    | pairtools dedup "
          f"        --max-mismatch 2 --mark-dups"
          f"        --output {pair}"
          f"        --output-dups {duplicate}")

    return pair, duplicate


@shell(
    storedir="results/pairs/sample"
)
def stat_pair(pair: File):
    stat = f"{pair.stem}.stat"

    Shell(f"pairtools stats --output {stat} {pair}")

    return stat


@shell(
    pubdir="results/cools/minres",
    memory="4 GB"
)
def pair_to_cool(self, pair: File, chromsizes: File):
    cool = f"{pair.stem}_{self.min_res / 1000}k.cool"

    Shell(f"{cat(pair)} | cooler cload pairs -c1 2 -p1 3 -c2 4 -p2 5 "
          f"    --assembly {chromsizes}:{self.min_res} - {cool}")

    return cool


@shell(
    storedir="results/cools"
)
def zoomify_cool(self, cool: File):
    resolutions = ','.join(self.resolutions)
    mcool = f"{cool.stem}.mcool"

    Shell(f"cooler zoomify --balance --resolutions {resolutions} --out {mcool} {cool}")

    return mcool


@shell(
    pubdir="results/features/{tags}",
    cpus=20,
    max_forks=1
)
def mcool_to_features(mcool: File):
    cool_100k = f"{mcool}::/resolutions/100000"
    cool_10k = f"{mcool}::/resolutions/10000"
    name = mcool.stem

    Shell(f"hictools compartments decomposition {cool_100k} {name}_100k_com.bed\n"
          f"hictools expected {cool_10k} {name}_10k_exp.bed\n"
          f"hictools peaks hiccups {cool_10k} {name}_10k_peaks.bed\n"
          f"hictools tads di-score {cool_10k} {name}_10k_discore.bed\n"
          f"hictools tads insu-score {cool_10k} {name}_10k_insu_score.bed\n")

    return "*"

@shell
def rename(run: Run):
    newname = f"{run.group}-{run.url.name}"
    Shell(f"ln -s {run.url} {newname}")
    return newname

def join_fn(run: Run):
    pass

@flow
def check_inputs(files):

    fastq_pairs = files \
                  | check_files | flatten | filter_(lambda x: x != False) \
                  | download | flatten \
                  | fastq_dump | flatten \
                  | rename \
                  | group(join_fn, 2) | select(1)
    return fastq_pairs


@flow
def hic_flow(inputs, assembly, enzyme):
    def select_group(pair: File):
        return pair.name.split('-')[0]

    fasta = fetch(assembly, type='genome')
    chromsizes = fetch(assembly, type="chromsizes")
    index = fetch(assembly, type="index", aligner="bwa")
    frag = digest(fasta, chromsizes, enzyme)

    # main step
    fastq_pairs = check_inputs(inputs)
    exp_pairs = map_parse_sort(index, chromsizes, fastq_pairs) | select(1)
    sample_pairs = exp_pairs \
                   | partial(select_pair, frag=frag) | select(0) \
                   | group(by=select_group) | merge_dedup_pair | select(0)
    features = sample_pairs \
               | partial(pair_to_cool, chromsizes=chromsizes) \
               | zoomify_cool | mcool_to_features
    # stats
    qc_results = fastq_pairs | fastqc | last | map_(lambda x: Path('results/multiqc')) | mutliqc
    pair_stats = [exp_pairs, sample_pairs] >> stat_pair | mix



config.raw_reads = {
    'sample1': [
        ['asdasd_1.fq', 'asdasd_2.fq'],
        "SRRdasdasd.sra",
        "SRRsdasdasdasd"
    ],
}

inputs = []
iter_list([], config.raw_reads, inputs)

inputs = Channel.from_list(inputs)
assemly = Channel.values('hg19')
enzyme = Channel.values('dnpIII')

runner, workflow = FlowRunner(hic_flow).run(inputs, assemly, enzyme)
# runner.execute()
workflow.graph.render('/store/qzhong/dag.test', view=True, format='pdf', cleanup=True)
