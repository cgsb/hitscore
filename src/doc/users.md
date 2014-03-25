
Dealing With Users
==================


New User
--------

A new user can be created in the DB:

- by the submission sheet import, *or*
- by creating a fresh record on: 
[/layout?type=person](https://gencore.bio.nyu.edu/layout?type=person)
(pick an existing user, copy the S-Expression, and click on 
*“You may add a new person”*, paste the S-Expr, modify it and submit).

To get access to the website:

- If the user has a NetID, and the NetID must be registered in the user's
record (the `login` field). **AND** the user must be added to `WSO1` (ask
`fas.bio.computing@nyu.edu` to add the user to the “`gencore app`”).
- If the user is not from NYU, we need to create an Custom password:
    - go to [/persons](https://gencore.bio.nyu.edu/persons)
    and click on the user's email address.
    - click on *“You may change your GenCore password”*
    - create a password and submit.
    - Email the user their new password, telling them to change it immediately
    by clicking on their ID in the top banner when they are logged-in (which is
    <https://gencore.bio.nyu.edu/self>).

To get access to data on Bowery:

- The user needs to have a Gencore account **AND** an account on Bowery part of
the `cgsb` Unix group (If the user is not yet in the `cgsb` group, you need to
ask `hpc@nyu.edu` to add the NetID to the `cgsb` group and to the `cgsb-s` PBS
queue).

Add User To Lanes
-----------------

A user is associated to a Lane in a HiSeq run when they are listed as “Contact”
in the submission sheet.

But often users forget each other, and later ask to be added to a run.

    $ hitscore production add-user-to-lanes
    Wrong arguments: need at least a user name !!
    Usage: hitscore <profile> add-user-to-lanes [-wet-run] <user> <spec>
    Where the specification is a list of either:
     * lane database IDs
     * 'FCIDXX:*' → (lanes 2 to 8)
     * 'FCIDXX:n-m' → (lanes n to m)
     * 'FCIDXX:i,j,k' → (lanes i, j, k)

Example (dry-run):

    hitscore production add-user-to-lanes grr42 FLWCLIDCXX:4,5,6,7

Then do the actual change:

    hitscore production add-user-to-lanes -wet-run grr42 FLWCLIDCXX:4,5,6,7

Very often this happens after the delivery of those lanes, so the deliver has
to be *redone*:

    hitscore production deliver -redo /data/cgsb/gencore/out/<PI>/<Delivery-directory>

This will reset the access rights with the new user.
