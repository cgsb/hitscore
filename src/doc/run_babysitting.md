Things To Do For Each Run
=========================


HiSeq 2500
----------

### Before The Run

- Create submission-sheet(s).
- Make sure the HiSeq hard-drives are almost empty (`D:` and `E:`).
- Make sure there is enough space in `Pod:/hiseq/`.

### At The Pool Submission Deadline

- Validate the Submission sheet(s).

### After The Run

#### Data transfers

Go to `Pod`, in `/hiseq`, check that the run is completed:
the file `/hiseq/$DIR/RTAComplete.txt` should exist.

Given the amount of files, creating a `tar.gz` before transfering is faster and
safer than `rsync`-ing directly.

    tar  --exclude=Data/Intensities/L00*/C*.1  -czf ~/$DIR.tar.gz $DIR

The `--exclude` option is to not save the intensities (but they stay on Pod
until we are sure they cannot be reused to another Basecalling).

The this should be transmitted to `Bowery`; the `gencore` user has a
`~/bin/rsync-tar.pbs` script.

    qsub -v DIR=$DIR ~/bin/rsync-tar.pbs

As of *Fri, 14 Mar 2014 15:09:31 -0400*, the script should be modified: it
contains `sm4431`'s email address and (Pod) `$HOME`.

Then the archive must be extracted, c.f. script `~/untar.pbs`, and
copied to `/data/cgsb/gencore-raw/HiSeq/` (backed-up directory).

We usually double-check the transfers by creating timestamped MD5 sums on both
`Pod` and `Bowery` (we have seen rsync returning `0` while failing to transfer
something, since we use the intermediary `tar.gz` this has not happened
though).

#### After The Transfers

Then usually, Hitscore takes over, see [Hitscore's usage
notes](./hitscore_usage.md).

Without Hitscore, the idea is to run CASAVA, on `Bowery`:

     module load casava/intel/1.8.2
     configureBclToFastq.pl --help

See `/data/cgsb/gencore/bcl_to_fastq` on `Butinah` for examples., but the idea
is:

- Create a `Samplesheet.csv`.
- Run `configureBclToFastq` → this creates an `Unaligned` directory, with a
`Makefile`.
- Run `make -j12` in a PBS job.


Example:

    configureBclToFastq.pl \
      --fastq-cluster-count 800000000 \
      --input-dir $IN_ROOT/$RUN/Data/Intensities/BaseCalls \
      --output-dir $OUT_ROOT/$RUN/Unaligned \
      --sample-sheet $OUT_ROOT/$RUN/SampleSheet.csv \
      --mismatches 1

When the run is dual-indexed, one must add the option
`--use-bases-mask 'Y*,I8,I8,Y*'`.

#### Delivery

The delivery without Hitscore is done by simply giving read access to the FASTQ
files inside `Unaligned` (c.f. `man setfacl`).

MiSeq
-----

### Before The Run

Just check that the hard-drive is not full.

### After The Run

Go to the machine and copy the run directory to the `gencore-raw` mounted
volume.

Then from `Pod` follow the same process as with the HiSeq (`tar`, `rsync`,
`untar`).

The FASTQ files are already there, one just needs to give access rights.

PGM
---

Log into the PGM, and do the backups to Bowery:

- The RAW files are in `/results/sn25080361/<run name>`, the file
`explog_final.txt` shows that the run is done.
- The FASTQ files are in `/results/analysis/output/Home`, the naming scheme is
not well-defined, one has to look for them.

The delivery is done on a case-by-case (people can download them form the web
interface, or may want them somewhere on `Bowery`).
