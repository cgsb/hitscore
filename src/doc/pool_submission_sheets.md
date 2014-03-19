
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
