
open Core.Std

open Hitscore
  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      

let v10_to_v11 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v10 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V10.Layout.dump_of_sexp dump_v10 |! ignore in


  let dump_v11 =
    let rec parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V10.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V11.Info.version ]
      | List [Atom "person"; List persons]  ->
        eprintf "Persons...: %d\n" (List.length persons) ;
        List [Atom "person";
              List (List.map persons (function
              | Atom a -> assert false
              | List l -> 
                List (List.map l ~f:(function
                | List [Atom "g_value"; l] ->
                  List [Atom "g_value"; (add_empty "affiliations" l)]
                | l -> l))))]
      | List l -> List (List.map l ~f:parse)
    in
    dump_v10
    |! add_empty "hiseq_statistics"
    |! add_empty "affiliation"
    |! parse
        
  in
  
  let () =
    try
      V11.Layout.dump_of_sexp dump_v11 |! ignore
    with e ->
      eprintf "Could not reparse in V11: %s\n" (Exn.to_string e)
  in
  
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v11)));
  ()

    
let v11_to_v12 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v11 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V11.Layout.dump_of_sexp dump_v11 |! ignore in


  let dump_v12 =
    let rec parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V11.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V12.Info.version ]
      | List l -> List (List.map l ~f:parse)
    in
    dump_v11
    |! parse
        
  in
  
  let () =
    try
      V12.Layout.dump_of_sexp dump_v12 |! ignore
    with e ->
      eprintf "Could not reparse in V12: %s\n" (Exn.to_string e)
  in
  
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v12)));
  ()
let () =
  match Array.to_list Sys.argv with
  | exec :: "v10-v11" :: file_in :: file_out :: [] ->
    v10_to_v11 file_in file_out
  | exec :: "v11-v12" :: file_in :: file_out :: [] ->
    v11_to_v12 file_in file_out
  | _ ->
    eprintf "usage: %s {v10-v11,v11-v12} <dump-in> <dump-out>\n" Sys.argv.(0)
