
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
    | List [Atom "g_evaluation"; l] -> List [Atom "g_evaluation"; (f l)]
    | l -> l))))

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
      | List [Atom "client_fastqs_dir"; List cfd]  ->
        eprintf "Client_fastqs_dir: %d\n" (List.length cfd) ;
        List [Atom "client_fastqs_dir";
              List (map_g_values cfd (add_string "host" "bowery.es.its.nyu.edu")) ]
      | List [Atom "bcl_to_fastq"; List b2fs]  ->
        eprintf "Bcl_to_fastqs: %d\n" (List.length b2fs) ;
        List [Atom "bcl_to_fastq";
              List (map_g_values b2fs (add_string "basecalls_path" "Data/Intensities/BaseCalls")) ]
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

let illumina_barcodes = [
  1, "ATCACG"; 2, "CGATGT"; 3, "TTAGGC"; 4, "TGACCA"; 5, "ACAGTG";
  6, "GCCAAT"; 7, "CAGATC"; 8, "ACTTGA"; 9, "GATCAG"; 10, "TAGCTT";
  11, "GGCTAC"; 12, "CTTGTA"; 13, "AGTCAA"; 14, "AGTTCC"; 15, "ATGTCA";
  16, "CCGTCC"; 17, "GTAGAG"; 18, "GTCCGC"; 19, "GTGAAA"; 20, "GTGGCC";
  21, "GTTTCG"; 22, "CGTACG"; 23, "GAGTGG"; 24, "GGTAGC"; 25, "ACTGAT";
  27, "ATTCCT";
]

let bioo_barcodes = [
  1,"CGATGT"; 2,"TGACCA"; 3,"ACAGTG"; 4,"GCCAAT"; 5,"CAGATC";
  6,"CTTGTA"; 7,"ATCACG"; 8,"TTAGGC"; 9,"ACTTGA"; 10,"GATCAG";
  11,"TAGCTT"; 12,"GGCTAC"; 13,"AGTCAA"; 14,"AGTTCC"; 15,"ATGTCA";
  16,"CCGTCC"; 17,"GTAGAG"; 18,"GTCCGC"; 19,"GTGAAA"; 20,"GTGGCC";
  21,"GTTTCG"; 22,"CGTACG"; 23,"GAGTGG"; 24,"GGTAGC"; 25,"ACTGAT";
  26,"ATGAGC"; 27,"ATTCCT"; 28,"CAAAAG"; 29,"CAACTA"; 30,"CACCGG";
  31,"CACGAT"; 32,"CACTCA"; 33,"CAGGCG"; 34,"CATGGC"; 35,"CATTTT";
  36,"CCAACA"; 37,"CGGAAT"; 38,"CTAGCT"; 39,"CTATAC"; 40,"CTCAGA";
  41,"GCGCTA"; 42,"TAATCG"; 43,"TACAGC"; 44,"TATAAT"; 45,"TCATTC";
  46,"TCCCGA"; 47,"TCGAAG"; 48,"TCGGCA";]

let v15_to_v16 file_in file_out =
  let open Sexplib.Sexp in
  let dump_v15 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () = V15.Layout.dump_of_sexp dump_v15 |! ignore in
  let dump_v16 =
    let parse = function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version] ->
        eprintf "Changed version: %s -> %s.\n" old_version V16.Info.version;
        List [Atom "version"; Atom V16.Info.version ]
      | List [Atom "barcode"; List barcodes]  ->
        eprintf "barcodes: %d\n" (List.length barcodes) ;
        let new_barcodes =
          let module M15 = struct
            type t = {
              kind: Layout.Enumeration_barcode_type.t;
              index: int option;
              position_in_r1: int option;
              position_in_r2: int option;
              position_in_index: int option;
              sequence: string option } with sexp
          end in
          let module M16 = struct
            type t = {
              (* kind: string; *)
              (* index: int option; *)
              read: string option;
              position: int option;
              sequence: string option;
              provider: string option;
              name: string option;
            } with sexp
            let of_15 v =
              let {M15. kind; index; position_in_r1; position_in_r2;
                    position_in_index; sequence } = v in
              match kind with
              | `illumina ->
                let i =
                  Option.value_exn index
                    ~message:"illumina barcode without index!" in
                begin match List.Assoc.find illumina_barcodes i with
                | Some s ->
                  { read = Some "I1";
                    position = Some 1;
                    sequence = Some s;
                    provider = Some "illumina";
                    name = Some "1" }
                | None -> failwithf "illumina barcode %d not found" i ()
                end
              | `bioo ->
                let i =
                  Option.value_exn index
                    ~message:"bioo barcode without index!" in
                begin match List.Assoc.find bioo_barcodes i with
                | Some s ->
                  { read = Some "I1";
                    position = Some 1;
                    sequence = Some s;
                    provider = Some "bioo";
                    name = Some "1" }
                | None -> failwithf "bioo barcode %d not found" i ()
                end
              | `custom ->
                let read, position =
                  match position_in_r1, position_in_r2, position_in_index with
                  | Some n, None, None -> Some "R1", Some n
                  | None, Some n, None -> Some "R2", Some n
                  | None, None, Some n -> Some "I1", Some n
                  | _, _, _ -> failwithf "unexpected barcode record"  ()
                in
                { read; position; sequence; provider = None; name = None }
              | `bioo_96 -> failwith "not implemented bioo 96"
              | `nugen ->
                assert (position_in_r1 = None && position_in_index = None
                        && position_in_r2 = None && index <> None);
                {read = None; position = None; sequence = None;
                 provider = Some "nugen";
                 name = Some (Int.to_string (Option.value_exn index))}
          end in
          map_g_values barcodes (fun g_value ->
                let v15 = M15.t_of_sexp g_value in
                (* eprintf "Got %s\n%!" v15.M15.kind; *)
                M16.(sexp_of_t (of_15 v15)))
        in
        List [Atom "barcode"; List new_barcodes]
      | List [Atom "stock_library"; List sls]  ->
        eprintf "Stocks: %d\n" (List.length sls) ;
        List [Atom "stock_library";
              List (map_g_values sls (function
                | Atom _ -> assert false
                | List fields ->
                  List (List.map fields ~f:(function
                    | List [Atom "p5_adapter_length"; sexp] ->
                      List [Atom "x_adapter_length"; sexp]
                    | List [Atom "p7_adapter_length"; sexp] ->
                      List [Atom "y_adapter_length"; sexp]
                    | List [Atom "truseq_control"; Atom boolean] ->
                      (* eprintf "truseq_control: %s\n%!" boolean; *)
                      List [Atom "truseq_control"; List [Atom boolean]]
                    | other -> other))))]
      | List l -> List l
    in
    begin match dump_v15 with
    | Atom  a -> assert false
    | List l ->
      let sorted = List.sort l ~cmp:(fun a b ->
        match a,b with
        | List [Atom va; _], List [Atom vb; _] -> - (compare va vb)
        | _ -> assert false) in
      List (List.map sorted ~f:parse)
      |> add_empty "pgm_input_library"
      |> add_empty "pgm_pool"
      |> add_empty "pgm_run"
      |> add_empty "pgm_raw"
    end
  in

  let () =
    try
      V16.Layout.dump_of_sexp dump_v16 |! ignore
    with e ->
      eprintf "Could not reparse in V16: %s\n" (Exn.to_string e)
  in

  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v16)));
  ()

let () =
  match Array.to_list Sys.argv with
  (*| exec :: "v13-v14" :: file_in :: file_out :: [] ->
    v13_to_v14 file_in file_out*)
  | exec :: "v14-v15" :: file_in :: file_out :: [] ->
    v14_to_v15 file_in file_out
  | exec :: "v15-v16" :: file_in :: file_out :: [] ->
    v15_to_v16 file_in file_out
  | _ ->
    eprintf "usage: %s {v14-v15,v15-v16} <dump-in> <dump-out>\n" Sys.argv.(0)
