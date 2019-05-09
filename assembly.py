import os
import shutil
import subprocess


def spades_cmd(reads, out):
    if len(reads) is 2:
        cmd = [
            "spades.py", "-1", reads[0], "-2", reads[1], "-t", "8", "-o", out, "--careful",
        ]
    else:
        cmd = [
            "spades.py", "-s", reads[0], "-t", "8", "-o", out, "--careful",
        ]
    return cmd


class Assembly:
    def __init__(self, fastq_dir, outdir):
        self.reads = [os.path.join(fastq_dir, i) for i in os.listdir(fastq_dir)]
        self.outdir = outdir
        self.contig_file = os.path.join(outdir, 'contigs.fasta')

    def denovo(self):
        if len(self.reads) > 2:
            self.reads = self._clean_barcode()
        cmd = spades_cmd(reads=self.reads, out=self.outdir)
        subprocess.call(cmd, stdout=subprocess.DEVNULL)

    def _clean_barcode(self):
        reads_size = {read: os.stat(read).st_size for read in self.reads}
        reads_size_sort = sorted(reads_size, key=lambda x: reads_size[x], reverse=True)
        return reads_size_sort[0:2]

    def move_contig(self, outdir):
        shutil.copy(self.contig_file, outdir)
