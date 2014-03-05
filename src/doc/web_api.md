
## Using The Gencore Web-API

### Get An Authentication Token

Authentication tokens are big random secrets shared between the server and you.

To create a new one, or delete/revoke an old one: go to
<https://gencore.bio.nyu.edu/self> (or click on your email address in the top
banner).

You can give names to authentication tokens, those names are used only for
display (for you to keep track of which token you are using).

### Call The API

#### Definitions

The base URL is <https://gencore.bio.nyu.edu/api>.

Arguments follow the URL, in a standard way. Here is their current
[Eliom](http://ocsigen.org/eliom/) definition:

```ocaml
Eliom_parameter.(
    opt (string "query") 
    ** opt (string "token")
    ** opt (string "user")
    ** opt (string "format")
    ** set string "filter_qualified_names"
 )
```

Their meanings are:

- `query` is the “name” of the query, the default value is `libraries` (and it
is for now the only one).
- `user` is the user identifier, it works like logging-in into the website, the
netID and any registered email address works.
- `token` is the authentication token (see previous section).
- `format` is the output format the default and, for now, only value is `"csv"`.
- `filter_qualified_names`, when `query` is `libraries`, one can filter the
output, by providing a set of regular expressions (Perl-like syntax), which are
applied successively on the “Qualified Names” (i.e. `project.name`) of the libraries.

In case of error, the query returns a `404` HTTP code, with a string describing
the error.

When succeeding (code `200`), the current CSV format, one row per  “delivered
library” (anyting except the library name can be empty):

- Library name.
- Project name.
- Library short description.
- Sample name.
- Organism name.
- Path to the READ-1 file on Bowery.
- Path to the READ-2 file on Bowery.
- Read count (may be zero even when there are reads, because of sys-admin
reasons, the webserver has trouble accessing Bowery information).

#### Examples:

Let's use 

    alias my_curl='curl -w "\ncode: %{http_code}\nct: %{content_type}\n"'

to show also the HTTP return code and the content-type of the answer.


This query tries to call the `libraries` without authentication:

    my_curl "https://gencore.bio.nyu.edu/api?query=libraries"

it results in a `404`:

```
wrong credentials: "Nope"
code: 404
ct: text/html; charset=utf-8
```

This query shows all the libraries of the user ue345, who has a valid
authentication token:

    my_curl "https://gencore.bio.nyu.edu/api?query=libraries&user=ue345&token=blablablabla398cablabla2682d26blablablad415eblablabladecblabla7b"

the result is a list of libraries, in `text/csv` format:

```
Libname01,Project_foo,"some description of the library",Sample_name01,Organism,/some/big/path/to/READ1.fastq.gz,/some/big/path/to/READ2.fastq.gz,read-count
Libname02,Project_bar,"description from sub-sheet",Sample_name04,C. Elegans,/some/big/path/to/READ1.fastq.gz,,42
...

code: 200
ct: text/csv; charset=utf-8
```

Then one can use `filter_qualified_names` to filter the libraries:


    my_curl "https://gencore.bio.nyu.edu/api?query=libraries&user=ue345&token=blablablabla398cablabla2682d26blablablad415eblablabladecblabla7b&filter_qualified_names=Project_foo.*"

which displays only the libraries matching `"Project_foo.*"`:

```
Libname01,Project_foo,"some description of the library",Sample_name01,Organism,/some/big/path/to/READ1.fastq.gz,/some/big/path/to/READ2.fastq.gz,read-count
...

code: 200
ct: text/csv; charset=utf-8
```

Also one can set the format to `json`:

    my_curl "https://gencore.bio.nyu.edu/api?query=libraries&user=ue345&token=blablablabla398cablabla2682d26blablablad415eblablabladecblabla7b&format=json"

And get the result as a Json array of objects describing the libraries.


