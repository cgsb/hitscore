#! /usr/bin/env ocamlscript
Ocaml.ocamlflags := ["-thread"];
Ocaml.packs := ["batteries"; "csv"; "pgocaml"; "pgocaml.syntax"]
--
open Batteries;; open Printf

let prog = Filename.basename Sys.argv.(0)
let usage = sprintf
"usage: %s metdata_dir

  Migrate data from google doc spreadsheet \"CGSB Genomics Core
  Metadata\" to new PostgreSQL database. This migration was done in
  Nov 2011.

  metadata_dir - Directory to which google spreadsheet has been
  exported in tab-delimited format. There should be one file per sheet
  in the overall spreadsheet. See code for additional details on
  expected format.

  Environment Variables: PostgreSQL environment variables should be
  set as necessary for PG'OCaml to connect to the appropriate
  database. The database should also be initialized with appropriate
  tables but all tables should be empty.
" prog

let metadata_dir = Sys.argv.(1)
let mkpath = List.fold_left Filename.concat metadata_dir
let load = Csv.load ~separator:'\t'

let main dbh =
  let tbl = load (mkpath ["Person.tsv"]) |> List.tl in
  let f row =
    match row with
      | [_;first;last;email] ->
          PGSQL(dbh)
            "INSERT INTO person (first_name,last_name,email)
             VALUES ($first,$last,$email)"
      | _ -> assert false
  in
  List.iter f tbl

;;
let dbh = PGOCaml.connect() in
main dbh
