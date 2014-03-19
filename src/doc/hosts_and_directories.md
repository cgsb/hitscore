
Servers, Directories, Clusters
==============================

- HiSeq writes locally to `D:` and `E:`
- `Pod`:
    - `/hiseq/` is mounted as `gencore-raw` on the HiSeq and the MiSeq.
    - `/hiseq/<RUN_DIRECTORY>` HiSeq and MiSeq run directories
        - MiSeq runs copied manually.
        - HiSeq runs copied by the machine, **with Intensities**.
    - `/hiseq/SAV_only/<RUN_DIRECTORY>` extracts of every HiSeq run, contain
    only the file `runParameters.xml` and the directory `InterOp/`
- `Bowery` (the `/data/cgsb/` tree is the part of `/scratch` that is backed-up):
    - `/scratch/gencore/<SEQUENCER_NAME>` scratch space where the run
    directories are *un-tarred* until they are not needed any more.<br/>
    For the HiSeq **without Intensities**.
    - `/data/cgsb/gencore-raw/<SEQUENCER_NAME>` backups (`targ.gz`) of the runs
    (**without Intensities**).
    - `/data/cgsb/gencore/in/`: place where the submission sheets and files are treated (*HiSeq-only*).
    - `/data/cgsb/gencore/vol/`: root directory for Hitscore (fully managed by the software).
    - `/data/cgsb/gencore/backup/`: place to put the backups of the Hitscore database.
    - `/data/cgsb/gencore/out/`: “output” directory
        - Managed with Hitscore for the HiSeq (set of symbolic links pointing
        to `/data/cgsb/gencore/vol`).
        - Managed manually for the MiSeq (copy FASTQ files form `/scratch/gencore/miseq-M02455/<MISEQ_RUN>`).

