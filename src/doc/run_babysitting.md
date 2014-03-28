Things To Do For Each Run
=========================


HiSeq 2500
----------

### Before The Run

- Remote Desktop is available on the HiSeq2500 at:
  - Host: 172.22.2.31
  - User: SBSUser
  - Password: sbs123
- Remote Desktop is available on the MiSeq at:
  - Host: 172.22.2.46
  - User: SBSUser
  - Password: sbs123
- Create submission-sheet(s).
- Make sure the HiSeq hard-drives are almost empty (`D:` and `E:`).
- Make sure there is enough space in `Pod:/hiseq/` (c.f. section about
“clean-up” below).

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

#### Keep SAV Files

In order to inspect older runs with *SAV*, wet-lab people need an extract of
each run directory.
They are all there: `/hiseq/SAV_only/`.
The idea is to copy the `runParameters.xml` file and the `InterOp` directory.

#### Clean-Up Pod

Usually before each run, we check that `Pod`'s mount is at least half-empty.

    $ df -h /hiseq
    Filesystem            Size  Used Avail Use% Mounted on
    /export/gencore-raw    10T  1.8T  8.3T  18% /hiseq

To clean-up, list the directories in `/hiseq` directories. Send an email to the
Gencore manager, with the status of each run directory (backed-up, delivered,
“some problem to investigate”, etc.).

When both sides (*wet* and *dry* labs) agree, they send an email to `root`
(`fas.bio.computing@nyu.edu`) to ask for the deletions of the directories.

This deletion gets rid of the Intensities (since they were not backed-up);
running the basecaller again becomes impossible.

MiSeq
-----

### Before The Run

Just check that the hard-drive is not full.

### After The Run

Go to the machine and copy the run directory to the `gencore-raw` mounted
volume.

Then from `Pod` follow the same process as with the HiSeq (`tar`, `rsync`,
`untar`).

`Untar` on bowery and move tar.gz to `/data/cgsb/gencore-raw/miseq-M02455/`


The FASTQ files are already there, in `/scratch/gencore/miseq-M02455/<DIR>/Data/Intensities/BaseCalls/`.

In `/data/cgsb/gencore/out/`

    mkdir -p <PI Name>/<Date -- Meaningful run-name>

One just needs to copy the `fastq.gz` files there, and give access rights:

    setfacl -m u:<netID1>:r,u:<netID2>:r *

In case it is a new P.I., there may be read/exec rights to setup on upper directories.

PGM
---

Data from PGM is backed up on bowery and the delivery is done on a case-by-case basis 
(either download from the web interface, or copy to specified location on `Bowery`).

Log into the PGM
ssh ionadmin@pgm1.bio.nyu.edu

On PGM,

RAW (.dat) run files are stored in `/results/sn25080361/<run name>`
- `explog_final.txt` in run folder indicates that the run is complete.

Processed run files are stored in `/results/analysis/output/Home/<run name>`
- `explog_final.txt` in run folder indicates that the run is complete.

FASTQ files are usually stored in `/results/analysis/output/Home/<run name>/plugin_out/downloads`

On Bowery,

Data is backed up in `/data/cgsb/gencore-raw/pgm-25080361`
