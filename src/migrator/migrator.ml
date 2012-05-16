
open Core.Std

open Hitscore
  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      
let v08_to_v10 file_in file_out =
  let dump_v08 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let module V08M = V08.Make (Flow) in

  let () =
    V08M.dump_of_sexp dump_v08 |! ignore in

  let dump_v10 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V08.Info.version ->
        List [Atom "version"; Atom V10.Info.version ]
          
      | List l ->
        List (List.map ~f:parse l) in
    dump_v08
    (* |! add_empty "record_fastx_quality_stats_result" *)
    |! parse
  in
  
  let () =
    try
      V10.Layout.dump_of_sexp dump_v10 |! ignore
    with e ->
      printf "Could not reparse in V10: %s" (Exn.to_string e)
  in
  
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v10)));
  ()

    
let () =
  match Array.to_list Sys.argv with
  | exec :: "v08-v10" :: file_in :: file_out :: [] ->
    v08_to_v10 file_in file_out 
  | _ ->
    eprintf "usage: %s {v08-v10} <dump-in> <dump-out>\n"
      Sys.argv.(0)
