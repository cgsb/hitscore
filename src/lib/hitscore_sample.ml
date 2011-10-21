open Batteries_uni;; open Printf;; open Biocaml

exception Error of string

type t = {
  name1 : string;
  investigator : string option;
}

let of_row (get : Table.getter) (row : Table.row) =
  {
    name1 = get row "Sample Name1";
    investigator = (
      match get row "investigator" with "" -> None | x -> Some x
    )
  }
