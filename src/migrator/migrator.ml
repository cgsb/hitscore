
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
  let () = V08M.dump_of_sexp dump_v08 |! ignore in

  let id = ref 0 in
  let id_map = ref [] in
  let get_id name oid =
    match List.Assoc.find !id_map (name, oid) with
    | Some i -> i
    | None ->
      incr id;
      id_map := ((name, oid), !id) :: !id_map;
      !id in
  let get_id_atom name oid = Sexplib.Sexp.Atom (string_of_int (get_id name oid)) in


  let dsl = V10.Meta.layout () in
  let result_type f =
    let open Hitscoregen_layout_dsl in
    match List.find_map dsl.nodes (function
    | Function (n, _, r) when n = f -> Some r
    | _ -> None)
    with
    | Some e ->
      (* eprintf "result of %s is %s\n%!" f e; *)
      "record_" ^ e
    | None -> failwithf "can't find result type of %S" f  () in
  let type_of_field name field =
    let open Hitscoregen_layout_dsl in
    let check_typed_value = function
      | (a, Record_name r) 
      | (a, Option (Record_name r))
      | (a, Array (Record_name r)) when a = field -> Some ("record_" ^ r)
      | (a, Volume_name r)
      | (a, Option (Volume_name r))
      | (a, Array (Volume_name r)) when a = field -> Some "g_volume"
      | _ -> None in 
    match List.find_map dsl.nodes (function
    | Function (n, args, r) when n = name ->
      List.find_map args check_typed_value
    | Record (n, fields) when n = name ->
      List.find_map fields check_typed_value
    | _ -> None)
    with
    | Some e -> e
    (* eprintf "result of %s is %s\n%!" f e; *)
    | None -> failwithf "can't find type of %s.%s" name field  () in
    

  
  let dump_v10 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V08.Info.version ->
        List [Atom "version"; Atom V10.Info.version ]

      | List [Atom "file_system"; List fs_items] ->
        let new_fs_items =
          List.map fs_items (function
          | List [
            List [Atom "volume_pointer"; List [List [Atom "id"; Atom oid]]];
            List [Atom "volume_kind"; kind_sexp];
            List [Atom "volume_content"; content_sexp] ] ->
            List [
              List [Atom "g_id"; get_id_atom "g_volume" (int_of_string oid)];
              List [Atom "g_kind"; kind_sexp];
              List [Atom "g_content"; content_sexp];
            ]
          | sexp ->
            failwithf "new_fs_items:\n  %s\n" (Sexp.to_string_hum sexp)  ()) in
        List [Atom "file_system"; List new_fs_items] 

      | List [Atom type_name; List values] ->
        let g_thing, name =
          match String.lsplit2 ~on:'_' type_name with
          | Some ("record", name) -> ("g_value", name)
          | Some ("function", name) -> ("g_evaluation", name)
          | _ -> failwithf "cannot split %s" type_name () in
        let new_values =
          List.map values (function
          | List v ->
            let is_dezoptionalized s =
              List.exists [ "g_created"; "g_last_modified";
                            "g_recompute_penalty"; "g_inserted" ] ((=) s) in
            let g_values, non_g_values =
              List.partition_map v (function
              | List [Atom "g_id"; Atom i] ->
                `Fst (List [Atom "g_id"; get_id_atom type_name (int_of_string i)])
              | List [Atom "g_result"; List [List [List [Atom "id"; Atom i]]]] ->
                `Fst (List [Atom "g_result";
                            List [List [List [Atom "id";
                                              get_id_atom (result_type name)
                                                (int_of_string i)]]]])
              | List [Atom n; List [sexp]] when is_dezoptionalized n ->
                `Fst (List [Atom n; sexp])
              | List [Atom n; sexp] when String.is_prefix n ~prefix:"g_" ->
                `Fst (List [Atom n; sexp])
              | s -> `Snd s) in
            let relinked_non_g_values =
              List.map non_g_values (function
              | List [Atom field; List [List [Atom "id"; Atom i]]] ->
                List [Atom field; List [List [Atom "id";
                                              get_id_atom (type_of_field name field)
                                                (int_of_string i)]]]
              | List [Atom field; List [List [List [Atom "id"; Atom i]]]] ->
                List [Atom field; List [List [List [Atom "id"; 
                                                    get_id_atom
                                                      (type_of_field name field)
                                                      (int_of_string i)]]]]
              | e -> e) in
              
            List (g_values @ [List [Atom g_thing; List relinked_non_g_values]])
          | Atom a -> failwithf "unexpected atom : %s " a () 
          ) in
        List [Atom name; List new_values]
          
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
