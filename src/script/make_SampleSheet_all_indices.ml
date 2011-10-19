#! /usr/bin/env ocamlscript
Ocaml.packs := ["batteries"]
--
open Batteries_uni;; open Printf;;

let indices = [
  "ATCACG";
  "CGATGT";
  "TTAGGC";
  "TGACCA";
  "ACAGTG";
  "GCCAAT";
  "CAGATC";
  "ACTTGA";
  "GATCAG";
  "TAGCTT";
  "GGCTAC";
  "CTTGTA"
]

let lanes = 1 -- 8 |> List.of_enum

let columns = [
  "FCID"; "Lane"; "SampleID"; "SampleRef";
  "Index"; "Description"; "Control"; "Recipe";
  "Operator"; "SampleProject"
]


;;
let flowcell_id = Sys.argv.(1) in

printf "%s\n" (String.concat "," columns);

List.iter (fun lane ->
  List.iter (fun index ->
    printf "%s,%d,%s_Lane%d,,%s,,N,,,Lane%d\n" flowcell_id lane index lane index lane
  ) indices
) lanes
