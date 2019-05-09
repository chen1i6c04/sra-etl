import os
import subprocess


class SequenceReadArchive:
    def __init__(self, accession, outdir):
        self.accession = accession
        self.outdir = outdir
        self.url = None
        self.sra_file = os.path.join(self.outdir, self.accession + '.sra')

    def make_url(self):
        url = 'ftp://ftp.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/{}/{}/{}/{}'.format(
            self.accession[0:3], self.accession[0:6], self.accession, self.accession + '.sra')
        self.url = url

    def download(self):
        subprocess.call(['wget', self.url, '-O', self.sra_file], stderr=subprocess.DEVNULL)

    def split(self, fastq_dir):
        cmd = ["fastq-dump", self.sra_file, '--outdir', fastq_dir, '--split-files', '--gzip']
        subprocess.call(cmd, stdout=subprocess.DEVNULL)

    def remove(self):
        os.remove(self.sra_file)
