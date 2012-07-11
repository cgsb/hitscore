
open Core.Std

open Hitscore
  
let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)
      

let v10_to_v11 file_in file_out =
  let dump_v10 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V10.dump_of_sexp dump_v10 |! ignore in


  let dump_v11 = Atom "brout" in

  
  let () =
    try
      V11.Layout.dump_of_sexp dump_v11 |! ignore
    with e ->
      printf "Could not reparse in V11: %s" (Exn.to_string e)
  in
  
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v11)));
  ()

    
let () =
  match Array.to_list Sys.argv with
  | exec :: "v08-v10" :: file_in :: file_out :: moves :: id_table :: [] ->
    v08_to_v10 file_in file_out moves id_table
  | exec :: "v10-v11" :: file_in :: file_out :: [] ->
    v10_to_v11 file_in file_out
  | _ ->
    eprintf "usage: %s {v08-v10} <dump-in> <dump-out> <moves-out> <id-table-out>\n"
      Sys.argv.(0);
    eprintf "usage: %s {v10-v11} <dump-in> <dump-out>\n"
      Sys.argv.(0)
