import os
import click
from celery import group
from core import sra_download_and_split, genome_assembly, profile


@click.command()
@click.argument('SRA_List', type=click.Path(exists=True))
@click.argument('outdir', type=click.Path(exists=True))
@click.option('--database', default=None)
def main(sra_list, outdir, database):
    with open(sra_list, 'r') as file:
        sra_list = file.read()
    sra_list = sra_list.splitlines()

    if database:
        result = group(
            (sra_download_and_split.s(accession, os.path.join(outdir, accession)) | genome_assembly.s() | profile.s(database)) for accession in sra_list
        )()
    else:
        result = group(
            (sra_download_and_split.s(accession, os.path.join(outdir, accession)) | genome_assembly.s()) for accession in sra_list
        )()


if __name__ == '__main__':
    main()
