
About Submission Sheets
=======================

Users Documentation
-------------------

This is what the users are supposed to know:
<http://biology.as.nyu.edu/object/biology.facilities.submissionform>

Notes About Fields
------------------

Some fields are in **bold** it means “mandatory”, but not all of those are
*actually* mandatory.

### Persons

The email is the most important, and the email used for users that already are
in the system *must* be the “Primary Email” in the
[database](https://gencore.bio.nyu/edu).

For *new* people mandatory fields are effectively mandatory, and the `NetID` is
reasonably important (useful for delivering data and for logging into the
website with NYU credentials).

### Invoicing

We need at least one (existing or new) P.I. email for a given percentage.

If any of the “Chart-field” fields is wrong, it will be actually ignored

### Files

All files are optional, and can be put in the system later.

### Pools

When one of concentrations or volumes is missing, or when the pooling does not
sum up to 100%, importing will be fine:

    ## ERRORS:
    * Pool A row: Cannot read an integer from 
    * Pooled percentages for Pool A do not sum up to 100

### Libraries

The error:

     * Library <NAME> is declared new but the (project.)name is already in use by: 40733

is **critical**: `Project.Name` should be unique (Name should be
Unix-filename-compliant, even spaces will break CASAVA).

The fields corresponding to the following errors are not really mandatory:

    * mandatory field not provided: Application [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - Max Fragment Size [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - Mean Fragment Size [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - Min Fragment Size [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - PDF [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - Well Number [LIB_2, LIB_1]
    * mandatory field not provided: Bioanalyzer - XAD [LIB_2, LIB_1]
    * mandatory field not provided: Is Stranded [LIB_2, LIB_1]
    * mandatory field not provided: Library Preparator Email [LIB_2, LIB_1]
    * mandatory field not provided: P5 Adapter Length [LIB_2, LIB_1]
    * mandatory field not provided: P7 Adapter Length [LIB_2, LIB_1]
    * mandatory field not provided: Protocol File [LIB_2, LIB_1]
    * mandatory field not provided: Protocol Name [LIB_2, LIB_1]

### Barcoding

Barcode information is **critical**, it is better to leave it all blank than to
try import an erroneous barcoding.

For example, importing will fail when this error has not been solved:

    * Barcode some random name:2 is not known [LIB_1]

because `"some random name"` is not a valid barcode provider.

Valid barcodes are defined in Hitscore's configuration file (master at
`~gencore/.config/hitscore/config.sexp` on Bowery).

The barcode columns go by 2, both fields must go together (the 4 of them is
also OK):

- `Barcode Provider`, `Barcode Number` → for “standard” barcodes in the
configuration file.
- `Custom Barcode Sequence`, `Custom Barcode Location` → for custom barcodes.

Custom barcodes can be more than *one*, for example: `AACTGC,AGGTT` as
“sequences” with `R1:1,I1:2` as “Location” mean that `AACTGC` is to be expected
on read 1 at position 1, and `AGGTT` is to be expected on the (first) index
read at position 2.

