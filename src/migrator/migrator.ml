
open Core.Std
let (|>) x f = f x

module Hitscore_threaded =
  Hitscore.Make(Sequme_flow_monad.Preemptive_threading_config)
open Hitscore_threaded

let v051_to_v06 file_in file_out =
  let dump_v051 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () =
    let module V051M = V051.Make (Flow) in
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
    let module V06M = V06.Make (Flow) in
    try
      V06M.dump_of_sexp dump_v06 |! ignore
    with e ->
      printf "Could not reparse in V6: %s" (Exn.to_string e)
  in
  
  let module V06M = V06.Make(Flow) in
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

  let module V06M = V06.Make (Flow) in
  let module V07M = V07.Make (Flow) in

  let () =
    V06M.dump_of_sexp dump_v06 |! ignore in

  let dump_v07 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V06.Info.version ->
        List [Atom "version"; Atom V07.Info.version ]

      | List [Atom "file_system"; List volume_sexps] ->
        let module FS06 = V06M.File_system in
        let module FS07 = V07M.File_system in
        printf "File system: %d volumes\n" (List.length volume_sexps);
        let fail = Failure "transforming volumes" in
        let v06_volumes =
          List.map volume_sexps FS06.volume_of_sexp in
        let v07_volumes =
          List.map v06_volumes (fun vol06 ->
            let {FS06.volume_entry; files} = vol06 in
            let {FS06.v_id; v_toplevel; v_hr_tag; v_content } = volume_entry in
            let trees06 = FS06.volume_trees vol06 |! Result.ok_exn ~fail in
            let trees07 =
              let rec f = function
                | FS06.File (s, t)          -> FS07.File (s, t)
                | FS06.Directory (s, t, tl) -> FS07.Directory (s, t, List.map ~f tl) 
                | FS06.Opaque (s, t)        -> FS07.Opaque (s, t) in
              List.map trees06 f in
            FS07.({
              volume_pointer = { id = v_id };
              volume_kind = kind_of_toplevel v_toplevel |! Result.ok_exn ~fail;
              volume_content = Tree (v_hr_tag, trees07);
            })) in
        let v07_sexps = List.map v07_volumes FS07.sexp_of_volume in
        List [Atom "file_system"; List v07_sexps]

      | List [Atom "record_client_fastqs_dir"; List records] ->
        let records07 =
          List.map records (function
          | Atom _ -> assert false
          | List l ->
            List (List.map l (function
            | List [Atom "dir"; Atom s] -> List [Atom "directory"; Atom s] 
            | s -> s)))
        in
        List [Atom "record_client_fastqs_dir"; List records07]

      | List l ->
        List (List.map ~f:parse l) in
    dump_v06
    |! add_empty "record_fastx_quality_stats_result"
    |! add_empty "function_fastx_quality_stats"
    |! add_empty "record_generic_fastqs"
    |! add_empty "function_coerce_b2f_unaligned"
    |! parse
  in
  
  let () =
    try
      V07M.dump_of_sexp dump_v07 |! ignore
    with e ->
      printf "Could not reparse in V7: %s" (Exn.to_string e)
  in
  
  let module V07M = V07.Make(Flow) in
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
