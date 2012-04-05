
open Core.Std
let (|>) x f = f x

module Hitscore_threaded =
  Hitscore.Make(Sequme_flow_monad.Preemptive_threading_config)
open Hitscore_threaded

  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      
let v07_to_v08 file_in file_out =
  let dump_v07 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let module V07M = V07.Make (Flow) in
  let module V08M = V08.Make (Flow) in

  let () =
    V07M.dump_of_sexp dump_v07 |! ignore in

  let dump_v08 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V07.Info.version ->
        List [Atom "version"; Atom V08.Info.version ]

      | List [Atom "record_person"; List persons] ->
        List [Atom "record_person";
              List (List.map persons (add_empty "password_hash"))]
          
      | List l ->
        List (List.map ~f:parse l) in
    dump_v07
    (* |! add_empty "record_fastx_quality_stats_result" *)
    |! parse
  in
  
  let () =
    try
      V08M.dump_of_sexp dump_v08 |! ignore
    with e ->
      printf "Could not reparse in V8: %s" (Exn.to_string e)
  in
  
  let module V08M = V08.Make(Flow) in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v08)));
  ()

    
let () =
  match Array.to_list Sys.argv with
  | exec :: "v07-v08" :: file_in :: file_out :: [] ->
    v07_to_v08 file_in file_out 
  | _ ->
    eprintf "usage: %s {v07-v08} <dump-in> <dump-out>\n"
      Sys.argv.(0)
