
open Core.Std

open Hitscore

let add_empty name =
  let open Sexplib.Sexp in
  function
  | Atom a -> assert false
  | List l ->
    List (List [Atom name; List []] :: l)



let map_g_values list_of_values ~f =
  let open Sexp in
  (List.map list_of_values (function
  | Atom a -> assert false
  | List l ->
    List (List.map l ~f:(function
    | List [Atom "g_value"; l] -> List [Atom "g_value"; (f l)]
    | l -> l))))

let v12_to_v13 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v12 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V12.Layout.dump_of_sexp dump_v12 |! ignore in

  let max_g_id =
    let curmax = ref 0 in
    let parse = function
      | List [Atom "version"; Atom old_version] -> ()
      | List [Atom other; List other_items] ->
        List.iter other_items ~f:begin function
        | Atom a -> assert false
        | List l ->
          List.iter l ~f:(function
          | List [Atom "g_id"; Atom is] ->
            (* eprintf "g_id: %s\n" is; *)
            curmax := max !curmax (Int.of_string is)
          | _ -> ())
        end
      | _ -> assert false
    in
    begin match dump_v12 with
    | Atom  a -> assert false
    | List l ->
      List.iter l ~f:parse
    end;
    !curmax
  in
  eprintf "max_g_id: %d\n" max_g_id;

  let new_barcodes = ref [] in
  let new_id =
    let current_max = ref max_g_id in
    fun () ->
      incr current_max;
      !current_max
  in

  let dump_v13 =
    let parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V12.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V13.Info.version ]
      | List [Atom "lane"; List lanes]  ->
        eprintf "Lanes...: %d\n" (List.length lanes) ;
        List [Atom "lane";
              List (map_g_values lanes (add_empty "pool_name")) ]
      | List [Atom "custom_barcode"; List l ] ->
        let oldones =
          List.map l (fun b ->
            V12.Layout.Record_custom_barcode.t_of_sexp b
            |! begin fun {V12.Layout.Record_custom_barcode.
                          g_id; g_created; g_last_modified; g_value } ->
              let {V12.Layout.Record_custom_barcode.
                   position_in_r1; position_in_index;
                   position_in_r2; sequence } = g_value in
              let g_value =
                {V13.Layout.Record_barcode.
                 position_in_r2 = position_in_r2;
                 position_in_r1 = position_in_r1;
                 position_in_index = position_in_index;
                 sequence = Some sequence;
                 kind = `custom;
                 index = None } in
              {V13.Layout.Record_barcode.g_id; g_created; g_last_modified; g_value}
            end |! V13.Layout.Record_barcode.sexp_of_t)
        in
        let newones =
          List.map !new_barcodes (fun (_, b) ->
            V13.Layout.Record_barcode.sexp_of_t b) in
        List [Atom "barcode"; List (oldones @ newones)]
      | List [Atom "stock_library"; List stock] ->
        eprintf "%d stock libs\n" (List.length stock);
        List [Atom "stock_library"; List (map_g_values stock begin function
        | Atom a -> assert false
        | List l ->
          (* eprintf "l : %s\n" (to_string (List l)); *)
          let barcode_type = ref `none in
          let barcodes = ref [] in
          let custom_barcodes = ref [] in
          let record = List.filter_map l (function
            | Atom a -> failwithf "atom: %s" a ()
            | List [Atom "barcode_type"; Atom bp] ->
              barcode_type :=
                V12.Layout.Enumeration_barcode_provider.of_string_exn bp;
              None
            | List [Atom "barcodes"; List l] ->
              barcodes :=
                List.map l
                (function Atom s -> Int.of_string s | _ -> assert false);
              None
            | List [Atom "custom_barcodes"; List l] ->
              custom_barcodes :=
                List.map l (function
                | List [List [Atom "id"; Atom s]] -> Int.of_string s
                | s ->
                  eprintf "sexp: %s\n" (to_string s);
                  assert false
                );
              None
            | s ->
              (* eprintf "sexp: %s\n" (to_string s); *)
              Some s
          ) in
          eprintf "type: %s, custom_ones: %d normal: %d\n"
            (V12.Layout.Enumeration_barcode_provider.to_string !barcode_type)
            (List.length !custom_barcodes)
            (List.length !barcodes);
          let barcoding =
            let custom_ones = !custom_barcodes in
            let classic_ones =
              let open V13.Layout.Record_barcode in
              List.filter_map !barcodes (fun index ->
                match List.Assoc.find !new_barcodes (!barcode_type, index) with
                | _ when !barcode_type = `none || !barcode_type = `custom -> None
                | Some {g_id; _} -> Some g_id
                | None ->
                  eprintf "type: %s, index: %d\n"
                    (V12.Layout.Enumeration_barcode_provider.to_string !barcode_type) index;
                  let b = {
                    g_id = new_id (); g_created = Time.now (); g_last_modified = Time.now ();
                    g_value = {
                      kind =
                        V12.Layout.Enumeration_barcode_provider.to_string !barcode_type
                        |! V13.Layout.Enumeration_barcode_type.of_string_exn;
                      index = Some index; position_in_r1 = None;
                      position_in_r2 = None; position_in_index = None; sequence = None; }
                  } in
                  new_barcodes := ((!barcode_type, index), b) :: !new_barcodes;
                  Some b.g_id
              ) in
            classic_ones @ custom_ones
          in
          let barcoding_sexp =
            List.map barcoding
              (fun i -> List [List [Atom "id"; Atom (Int.to_string i)]]) in
          List (List [Atom "barcoding"; List [List barcoding_sexp]] :: record)
        end)]
      | List l -> List l
    in
    begin match dump_v12 with
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
      V13.Layout.dump_of_sexp dump_v13 |! ignore
    with e ->
      eprintf "Could not reparse in V13: %s\n" (Exn.to_string e)
  in

  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v13)));
  ()

let v13_to_v14 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v13 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V13.Layout.dump_of_sexp dump_v13 |! ignore in

  let dump_v14 =
    let parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V13.Info.version ->
        eprintf "Changed version.\n"; List [Atom "version"; Atom V14.Info.version ]
      | List [Atom "person"; List l ] ->
        List [Atom "person"; List (map_g_values l (add_empty "auth_tokens"))]
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

let () =
  match Array.to_list Sys.argv with
  | exec :: "v12-v13" :: file_in :: file_out :: [] ->
    v12_to_v13 file_in file_out
  | exec :: "v13-v14" :: file_in :: file_out :: [] ->
    v13_to_v14 file_in file_out
  | _ ->
    eprintf "usage: %s {v12-v13,v13-v14} <dump-in> <dump-out>\n" Sys.argv.(0)
