#! /usr/bin/env ocamlscript
Ocaml.packs := ["batteries"; "biocaml"; "sequme"]
--
open Batteries_uni;; open Printf;; open Biocaml;; open Sequme
module StringMap = Map.StringMap

type histogram = int StringMap.t
    (** Map from index to count of times it has occurred. *)

let total map =
  StringMap.fold (fun _ y tot -> y + tot) map 0

let print map =
  let tot = total map in
  let tot_float = float_of_int tot in
  StringMap.iter (fun index count ->
    printf "%s\t%d\t%.2f\n" index count (float_of_int count /. tot_float)
  ) map
  ;
  printf "TOTAL\t%d\t1.0\n" tot


let increment index map =
  let prev_count =
    try StringMap.find index map
    with Not_found -> 0
  in
  StringMap.add index (prev_count + 1) map

let f ans (seq_id,_,_,_) =
  let seq_id = Illumina.Fastq.sequence_id_of_string seq_id in
  increment seq_id.Illumina.Fastq.index ans

let run file =
  let inp = open_in file in
  inp |> Fastq.enum_input
  |> Enum.fold f StringMap.empty
  |> print

;;
run Sys.argv.(1)
