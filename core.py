import sys
import shutil
import configparser
from pathlib import Path as os_path
from celery import Celery
from assembly import Assembly
from sra import SequenceReadArchive

config = configparser.ConfigParser()
config.read('config.txt')
profiling_program = config['Config']['profiling']
celery_backend = config['Config']['backend']
celery_broker = config['Config']['broker']

sys.path.append(profiling_program)
from src.algorithms import profiling


app = Celery(backend=celery_backend, broker=celery_broker)


@app.task
def sra_download_and_split(accession, outdir):
    fastq_dir = os_path(outdir, 'fastq')
    sra = SequenceReadArchive(accession=accession, outdir=outdir)
    sra.make_url()
    sra.download()
    sra.split(fastq_dir)
    sra.remove()
    return fastq_dir, outdir


@app.task
def genome_assembly(args):
    fastq_dir, outdir = args
    contig_dir = os_path(outdir, 'contig')
    assembly = Assembly(fastq_dir=fastq_dir, outdir=outdir)
    assembly.denovo()
    shutil.rmtree(fastq_dir)
    assembly.move_contig(contig_dir)
    shutil.rmtree(assembly.outdir)
    return contig_dir, outdir


@app.task
def profile(args, database):
    contig_dir, outdir = args
    profile_dir = os_path(outdir, 'profile')
    os_path.mkdir(profile_dir, parents=True, exist_ok=True)
    profiling.profiling(profile_dir, contig_dir, database, threads=1, occr_level=0,
                        enable_adding_new_alleles=True, generate_profiles=True, debug=False)

