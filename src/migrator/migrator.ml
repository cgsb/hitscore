
open Core.Std

open Hitscore
  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      

    
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

let v12_to_v13 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v12 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V12.Layout.dump_of_sexp dump_v12 |! ignore in


  let dump_v13 =
    let rec parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V12.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V13.Info.version ]
      | List [Atom "lane"; List lanes]  ->
        eprintf "Lanes...: %d\n" (List.length lanes) ;
        List [Atom "lane";
              List (List.map lanes (function
              | Atom a -> assert false
              | List l -> 
                List (List.map l ~f:(function
                | List [Atom "g_value"; l] ->
                  List [Atom "g_value"; (add_empty "pool_name" l)]
                | l -> l))))]
      | List l -> List (List.map l ~f:parse)
    in
    dump_v12
    |! parse
  in
  
  let () =
    try
      V13.Layout.dump_of_sexp dump_v13 |! ignore
    with e ->
      eprintf "Could not reparse in V13: %s\n" (Exn.to_string e)
  in
  
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v13)));
  ()

let () =
  match Array.to_list Sys.argv with
  | exec :: "v11-v12" :: file_in :: file_out :: [] ->
    v11_to_v12 file_in file_out
  | exec :: "v12-v13" :: file_in :: file_out :: [] ->
    v12_to_v13 file_in file_out
  | _ ->
    eprintf "usage: %s {v11-v12,v12-v13} <dump-in> <dump-out>\n" Sys.argv.(0)
