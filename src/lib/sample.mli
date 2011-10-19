open Batteries_uni

exception Error of string

type t = {
  name1 : string;
  investigator : string option;
}

val of_row : Biocaml.Table.getter -> Biocaml.Table.row -> t
