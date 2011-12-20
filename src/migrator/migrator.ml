
open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
open Hitscore_threaded

let v011_to_v02 file_in file_out =
  let module V011 = V011.Make(Result_IO) in
  let module V02 = V02.Make(Result_IO) in
  let dump_v011 = In_channel.(with_file file_in ~f:input_all) in
  let s011 = V011.dump_of_sexp Sexplib.Sexp.(of_string dump_v011) in
  printf "s011 version: %s\n" s011.V011.version;
  ()

let () =
  match Array.to_list Sys.argv with
  | exec :: "v011-v02" :: file_in :: file_out ->
    v011_to_v02 file_in file_out 
  | _ ->
    eprintf "usage: %s {v011-v02} <dump-in> <dump-out>\n" Sys.argv.(0)
