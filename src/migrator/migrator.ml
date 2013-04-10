
open Core.Std

open Hitscore

let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)

let add_string name value =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; Atom value] :: l)


let map_g_values list_of_values ~f =
  let open Sexp in
  (List.map list_of_values (function
  | Atom a -> assert false
  | List l ->
    List (List.map l ~f:(function
    | List [Atom "g_value"; l] -> List [Atom "g_value"; (f l)]
    | l -> l))))
let v13_to_v14 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v13 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V13.Layout.dump_of_sexp dump_v13 |! ignore in

  let dump_v14 =
    let now = Time.(now () |! to_string) in
    let parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V13.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V14.Info.version ]
      | List [Atom "person"; List l ] ->
        List [Atom "person"; List (map_g_values l (fun v ->
          add_empty "auth_tokens" v |! add_empty "user_data"))]
      | List [Atom "file_system"; List l ] ->
        let f = function
          | Atom s -> failwithf "atom : %s" s ()
          | List l  ->
            List ((List [Atom "g_last_modified"; Atom now]) :: l)
        in
        List [Atom "file_system"; List (List.map l ~f)]
      | List l -> List l
    in
    begin match dump_v13 with
    | Atom  a -> assert false
    | List l ->
      let sorted = List.sort l ~cmp:(fun a b ->
        match a,b with
        | List [Atom va; _], List [Atom vb; _] -> - (compare va vb)
        | _ -> assert false) in
      List (List.map sorted ~f:parse) |! add_empty "authentication_token"
    end
  in

  let () =
    try
      V14.Layout.dump_of_sexp dump_v14 |! ignore
    with e ->
      eprintf "Could not reparse in V14: %s\n" (Exn.to_string e)
  in

  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v14)));
  ()

let v14_to_v15 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v14 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V14.Layout.dump_of_sexp dump_v14 |! ignore in
  let dump_v15 =
    let parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V14.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V15.Info.version ]
      | List [Atom "hiseq_run"; List hiseq_runs]  ->
        eprintf "Hiseq_runs: %d\n" (List.length hiseq_runs) ;
        List [Atom "hiseq_run";
              List (map_g_values hiseq_runs (add_string "sequencer" "CGSB-HS2000-1")) ]
      | List l -> List l
    in
    begin match dump_v14 with
    | Atom  a -> assert false
    | List l ->
      let sorted = List.sort l ~cmp:(fun a b ->
        match a,b with
        | List [Atom va; _], List [Atom vb; _] -> - (compare va vb)
        | _ -> assert false) in
      List (List.map sorted ~f:parse)
    end
  in

  let () =
    try
      V15.Layout.dump_of_sexp dump_v15 |! ignore
    with e ->
      eprintf "Could not reparse in V15: %s\n" (Exn.to_string e)
  in

  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v15)));
  ()


let () =
  match Array.to_list Sys.argv with
  | exec :: "v13-v14" :: file_in :: file_out :: [] ->
    v13_to_v14 file_in file_out
  | exec :: "v14-v15" :: file_in :: file_out :: [] ->
    v14_to_v15 file_in file_out
  | _ ->
    eprintf "usage: %s {v13-v14,v14-v15} <dump-in> <dump-out>\n" Sys.argv.(0)
