Clean Up The Gencore Directory
==============================

On The Google Drive, see the documents:

- [`Backups_and_data_retention`](https://docs.google.com/a/nyu.edu/document/d/1xDH79_lcKzhVdFlsYMtxUi8ChxT65ZdMXZ-0HfqWEHc/edit#heading=h.aqmrt7i08al7)
- [Raw data backups](https://docs.google.com/a/nyu.edu/spreadsheet/ccc?key=0AlgGw5GklnXhdDNIbzJITVAxclJMUU95MHAzUUFFYlE#gid=1)

The raw data backups document is updated with:

    \ls -1 -l -H *gz | awk '{ printf $9"\t"$5"\n" }'


HiSeq Data
----------

In `/data/cgsb/gencore-raw/hiseq-700911`, we keep backups up to 4 months after
delivery.

On can go to:
[/layout?type=client_fastqs_dir](https://gencore.bio.nyu.edu/layout?type=client_fastqs_dir)

- order row by  `g_last_modified`
- check what is younger than *4 months*
- now you know what can be potentially deleted

PGM
---

It there: `/data/cgsb/gencore-raw/pgm-25080361`, just pick the runs older than
6 months.


