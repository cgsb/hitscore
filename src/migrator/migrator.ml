
open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
open Hitscore_threaded

let v051_to_v06 file_in file_out =
  let dump_v051 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () =
    let module V051M = V051.Make (Result_IO) in
    V051M.dump_of_sexp dump_v051 |! ignore in

  let dump_v06 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V051.Info.version ->
        List [Atom "version"; Atom V06.Info.version ] 
      | List l ->
        List (List.map ~f:parse l) in
    let add_hs_runs = function
      | Atom a -> assert false
      | List l ->
        List (List [Atom "record_hiseq_run"; List []] :: l) in
    parse (add_hs_runs dump_v051) in
  
  let () =
    let module V06M = V06.Make (Result_IO) in
    try
      V06M.dump_of_sexp dump_v06 |! ignore
    with e ->
      printf "Could not reparse in V6: %s" (Exn.to_string e)
  in
  
  let module V06M = V06.Make(Result_IO) in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v06)));
  ()
  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      
let v06_to_v07 file_in file_out =
  let dump_v06 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () =
    let module V06M = V06.Make (Result_IO) in
    V06M.dump_of_sexp dump_v06 |! ignore in

  let dump_v07 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V06.Info.version ->
        List [Atom "version"; Atom V07.Info.version ] 
      | List l ->
        List (List.map ~f:parse l) in
    dump_v06
    |! add_empty "record_fastx_results"
    |! add_empty "function_fastx_quality_stats"
    |! add_empty "record_generic_fastqs"
    |! add_empty "function_coerce_b2f_unaligned"
    |! parse
  in
  
  let () =
    let module V07M = V07.Make (Result_IO) in
    try
      V07M.dump_of_sexp dump_v07 |! ignore
    with e ->
      printf "Could not reparse in V7: %s" (Exn.to_string e)
  in
  
  let module V07M = V07.Make(Result_IO) in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v07)));
  ()

    
let () =
  match Array.to_list Sys.argv with
  | exec :: "v051-v06" :: file_in :: file_out :: [] ->
    v051_to_v06 file_in file_out 
  | exec :: "v06-v07" :: file_in :: file_out :: [] ->
    v06_to_v07 file_in file_out 
  | _ ->
    eprintf "usage: %s {v051-v06,v06-v07} <dump-in> <dump-out>\n"
      Sys.argv.(0)
