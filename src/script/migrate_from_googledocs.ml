#! /usr/bin/env ocamlscript
Ocaml.ocamlflags := ["-thread"];
Ocaml.packs := ["hitscore"; "csv"; "pcre"]
--

open Core.Std
let (|>) x f = f x

let illumina_barcodes = [
  1, "ATCACG";
  2, "CGATGT";
  3, "TTAGGC";
  4, "TGACCA";
  5, "ACAGTG";
  6, "GCCAAT";
  7, "CAGATC";
  8, "ACTTGA";
  9, "GATCAG";
  10, "TAGCTT";
  11, "GGCTAC";
  12, "CTTGTA"
]
let bioo_barcodes = [
  1 ,"CGATGT";
  2 ,"TGACCA";
  3 ,"ACAGTG";
  4 ,"GCCAAT";
  5 ,"CAGATC";
  6 ,"CTTGTA";
  7 ,"ATCACG";
  8 ,"TTAGGC";
  9 ,"ACTTGA";
  10,"GATCAG";
  11,"TAGCTT";
  12,"GGCTAC";
  13,"AGTCAA";
  14,"AGTTCC";
  15,"ATGTCA";
  16,"CCGTCC";
  17,"GTAGAG";
  18,"GTCCGC";
  19,"GTGAAA";
  20,"GTGGCC";
  21,"GTTTCG";
  22,"CGTACG";
  23,"GAGTGG";
  24,"GGTAGC";
  25,"ACTGAT";
  26,"ATGAGC";
  27,"ATTCCT";
  28,"CAAAAG";
  29,"CAACTA";
  30,"CACCGG";
  31,"CACGAT";
  32,"CACTCA";
  33,"CAGGCG";
  34,"CATGGC";
  35,"CATTTT";
  36,"CCAACA";
  37,"CGGAAT";
  38,"CTAGCT";
  39,"CTATAC";
  40,"CTCAGA";
  41,"GCGCTA";
  42,"TAATCG";
  43,"TACAGC";
  44,"TATAAT";
  45,"TCATTC";
  46,"TCCCGA";
  47,"TCGAAG";
  48,"TCGGCA";
]


let prog = Filename.basename Sys.argv.(0)
let usage = sprintf
"usage: %s metdata_dir

  Migrate data from google doc spreadsheet \"CGSB Genomics Core
  Metadata\" to new PostgreSQL database. This migration was done in
  Nov 2011.

  metadata_dir - Directory to which google spreadsheet has been
  exported in tab-delimited format. There should be one file per sheet
  in the overall spreadsheet. See code for additional details on
  expected format.

  Environment Variables: PostgreSQL environment variables should be
  set as necessary for PG'OCaml to connect to the appropriate
  database. The database should also be initialized with appropriate
  tables but all tables should be empty.
" prog

module Usual_thread = struct
  include PGOCaml.Simple_thread

  let err = eprintf "%s%!"
  let map_s f l = List.map ~f l

end      
module Hitscore_db = Hitscore_db_access.Make(Usual_thread)
module PGOCaml = Hitscore_db.PGOCaml


let pcre_matches rex str = 
  try ignore (Pcre.exec ~rex str); true with Not_found -> false

let sanitize_for_filename str = 
  String.concat_map ~sep:"" str
    ~f:(function
      | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' as c -> String.of_char c
      | _ -> "")

let optify_string s =
  if s = "" then None else Some s

let main metadata_prefix dbh =
  
  (* let mkpath = List.fold_left ~f:Filename.concat ~init:metadata_dir in *)
  let load = Csv.load ~separator:',' in
  let load_table t =
    load (sprintf "%s%s.csv" metadata_prefix t) |> Csv.square |> List.tl_exn in
  let migration_note = sprintf "(From_migration %S)" Time.(to_string (now ())) in
  let persons =
    let note = migration_note in
    let nyu_login_email = Pcre.regexp "[a-z]+[0-9]+@nyu.edu" in
    let tbl = load_table "Person" in
    List.map tbl ~f:(function
      | [first;surname;email;nickname] ->
        let nickname = if nickname = "" then None else Some nickname in
        let login =
          if pcre_matches nyu_login_email email then
            List.hd (String.split ~on:'@' email)
          else
            begin match surname with
            | "Agarwal" -> Some "aa144"
            | "Mondet" -> Some "sm4431"
            | _ -> None
            end 
        in
        let id  : Hitscore_db.Record_person.t = 
          Hitscore_db.Record_person.add_value
            ~print_name:(sprintf "%s %s" first surname)
            ~family_name:surname ~note ?login
            ~email ?nickname ~dbh in
        (surname, id)
      | _ -> failwith "persons") in
  let organisms = ref [] in
  let samples = ref [] in
  let protocols = ref [] in
  let stock_libraries = ref [] in
  let try_previous assoc_list str ~new_one =
    if str = "" then None
    else
      begin match List.Assoc.find !assoc_list str with
      | Some id -> Some id
      | None ->
        let id = new_one str in
        assoc_list := (str, id) :: !assoc_list;
        Some id
      end in
  
  let libraries =
    let tbl = load_table "Library" in
    List.map tbl ~f:(function
      | [id; vial_id; library_id; sample_id1; sample_id2; protocol;
        organism; contact1; contact2; contact3; contact4; contact5;
        application; stranded; control_type; truseq_control_used;
        rna_seq_control; truseq_barcode; bioo_barcode;
        library_submitted; library_quantification_method;
        library_conc__nm____lab; library_volume__ul_;
        fragment_size_qc_method; pdf_file_path; xad_file_path;
        well_number; avg_library_fragment_size;
        fragment_size_qc_method_2; pdf_file_path_2; xad_file_path_2;
        well_number_2; avg_library_fragment_size_2;
        read_type; read_length; note ] ->
        let organism =
          try_previous organisms organism ~new_one:(fun str ->
            Hitscore_db.Record_organism.add_value
              ~name:organism ~dbh ?informal:None
              ~note:(migration_note ^ "(TODO Fix_names)")
          ) in
        let sample = 
          try_previous samples sample_id1 ~new_one:(fun str ->
            Hitscore_db.Record_sample.add_value ~dbh
              ~name:str ?organism ~note:migration_note) in
        let protocol =
          try_previous protocols protocol ~new_one:(fun str ->
            Hitscore_db.Record_protocol.add_value ~dbh
              ~name:str ~note:migration_note
              ~doc:(Hitscore_db.File_system.add_volume ~dbh
                      ~kind:`protocol_directory
                      ~hr_tag:(sanitize_for_filename str)
                      ~files:[])) in
        let stock_library =
          let note =
            migration_note ^ 
              (if note = "" then "" else sprintf "(spread_sheet_note %S)" note) in
          let application = optify_string application in
          let truseq_control = 
            match truseq_control_used with
            | "TRUE" -> true
            | "FALSE" -> false
            | "" -> false
            | s -> 
              failwithf "cannot recognize the truseq_control_used: %S" s ()
          in
          let stranded =
            match stranded with
            | "TRUE" -> true
            | "FALSE" -> false
            | "" -> false
            | s -> 
              failwithf "cannot recognize the `stranded': %S" s ()
          in
          let rnaseq_control = optify_string rna_seq_control in
          let read_length_1, read_length_2 =
            match read_type, read_length with
            | "PE", "50x50" ->   (50l,  Some 50l) 
            | "PE", "100x100" -> (100l, Some 100l)
            | t, l ->
              if library_id = "PhiX_v3" then
                (100l, Some 100l)
              else
                failwithf "[%s] did not get read_length's from (%S, %S)" 
                  library_id t l () in
          let barcode_type, barcodes =
            let i32 s = try Int32.of_string s with e -> 
              failwithf "Int32.of_string %S failed" s () in
            match truseq_barcode, bioo_barcode with
            | "", "" -> `none, [| |]
            | "", bb ->
              `bioo, [| i32 bb |]
            | "AD004,AD006", "" -> `illumina, [| 4l; 6l |]
            | ib, "" ->
              begin match String.chop_prefix ib ~prefix:"AD0" with
              | Some s ->
                `illumina, [| i32 s |]
              | None ->
                `illumina, [| i32 ib |]
              end
            | t, l ->
                failwithf "[%s] did not get barcodes from (%S, %S)" 
                  library_id t l () in
        
          try_previous stock_libraries library_id ~new_one:(fun str ->
            Hitscore_db.Record_stock_library.add_value ~dbh
              ~name:str ?sample ?protocol ?application
              ~stranded ~note ?rnaseq_control
              ~truseq_control ~read_length_1 ?read_length_2
              ~barcode_type ~barcodes
          ) in
        let input_library: Hitscore_db.Record_input_library.t =
          let submission_date =
            match String.split ~on:'/' library_submitted with
            | [ m; d; y ] ->
              Time.of_string (sprintf "%d-%02d-%02d 00:00:00-05:00" 
                                (int_of_string y)
                                (int_of_string m)
                                (int_of_string d))
            | l ->
              if pcre_matches  (Pcre.regexp "Niki.*") library_id then
                Time.of_string "2011-10-03 00:00:00-05:00"
              else if pcre_matches (Pcre.regexp "Paul.*") library_id then
                Time.of_string "2011-10-03 00:00:00-05:00"
              else
                Time.of_string "2011-06-24 00:00:00-05:00"
          in
          let contacts =
            Array.filter_map [|
              contact1;
              contact2;
              contact3;
              contact4;
              contact5; |] ~f:(fun ctct ->
                if ctct = "" then None else
                  Some (List.Assoc.find_exn persons ctct))
          in
          let volume_uL =
            Option.map (optify_string library_volume__ul_) ~f:Float.of_string in
          let concentration_nM = 
            Option.map (optify_string library_conc__nm____lab) ~f:Float.of_string in
          Hitscore_db.Record_input_library.add_value ~dbh
            ~library:(Option.value_exn stock_library) 
            ~submission_date ~note
            ~contacts ?volume_uL ?concentration_nM ~user_db:[| |]
        in
        eprintf "[%s] %s %s %s %s %s\n"  library_id
          fragment_size_qc_method pdf_file_path xad_file_path
          well_number avg_library_fragment_size;
        eprintf "[%s] %s %s %s %s %s\n"  library_id
          fragment_size_qc_method_2 pdf_file_path_2 xad_file_path_2
          well_number_2 avg_library_fragment_size_2;

        (library_id, sample_id1, input_library)
      | _ -> failwith "libraries don't fit") in
  let flowcells_stage_1 =
    let fcs = ref [] in
    let tbl = load_table "Flowcell" in
    List.iter tbl ~f:(function
      | [ fcid; lane; library_id; sample_name1; sequencing_started_;
        sequencing_completed; truseq_cluster_kit___barcode;
        truseq_sbs_kit___barcode; multiplexing_kit___barcode] ->
        begin match List.Assoc.find !fcs fcid with
        | Some lanes -> lanes := (fcid, lane, sample_name1) :: !lanes
        | None -> fcs := (fcid, ref [(fcid, lane, sample_name1)]) :: !fcs
        end
      | _ -> failwith "flowcells don't fit");
    List.rev_map !fcs ~f:(fun (f, r) -> (f, List.rev !r))
  in
  let flowcells =
    List.map flowcells_stage_1 ~f:(fun (fcid, runplan) ->
      let lanes =
        let libraries_of_lane n =
          List.filter_map runplan ~f:(fun (_, lane, sample_name1) ->
            if lane = string_of_int (n + 1) then 
              begin match 
                  List.find libraries (fun (l, s, i) ->
                    l = sample_name1 || s = sample_name1) with
                  | Some (l, s, i) -> Some i
                  | None ->
                    failwithf "Can't find the %S library" sample_name1 ()
              end
            else 
              None) in
        Array.init 8 (fun i ->
          Hitscore_db.Record_lane.add_value ~dbh
            ?seeding_concentration:None
            ?total_volume:None
            ~libraries:(Array.of_list (libraries_of_lane i))
            ~pooled_percentages:[| |])
      in
      Hitscore_db.Record_flowcell.add_value ~dbh
        ~serial_name:fcid ~lanes)
  in
  (persons, flowcells) |> ignore

let () =
  match Array.to_list Sys.argv with
  | exec :: dir :: [] -> 
    let dbh = PGOCaml.connect() in
    main dir dbh;
    let run_psql s =
      sprintf "echo '<h3>%s</h3>' >> ttt.html\n\
               psql --html -c '%s' >> ttt.html\n" s s
    in
    let report l = 
      Unix.system 
        (sprintf "rm ttt.html\n\
                 %s\n" (String.concat ~sep:"\n" (List.map l run_psql))) |> ignore in
    let all = sprintf "select * from %s;" in
    report [
      all "person";
      all "protocol";
      all "organism";
      all "sample";
      all "stock_library";
      all "input_library";
      all "lane";
      all "flowcell";
      all "g_volume";
    ];
    Out_channel.with_file "uuuu.brtx" ~f:(fun o ->
      let open Hitscore_db.Record_flowcell in
      List.iter (get_all ~dbh) (fun f ->
        let { serial_name ; lanes } = get_fields (cache_value ~dbh f) in
        fprintf o "{section|Flowcell %s}\n" serial_name;
        fprintf o "{begin table 4}\n";
        fprintf o "{c h|Lane} {c h|Lib} {c h|Sample} {c h|Contacts}\n";
        Array.iteri lanes ~f:(fun i l ->
          let open Hitscore_db.Record_lane in
          let { libraries; _ } = get_fields (cache_value ~dbh l) in
          fprintf o "{c h %dx1|%d}\n" (Array.length libraries) i;
          Array.iter libraries ~f:(fun il ->
            let open Hitscore_db.Record_input_library in
            let { library; contacts; _ } = get_fields (cache_value ~dbh il) in
            let open Hitscore_db.Record_stock_library in
            let { name; sample } = get_fields (cache_value ~dbh library) in
            fprintf o "  {c|%s}" name;
            begin match sample with
            | None -> fprintf o "{c|{i|NO SAMPLE}}"
            | Some sample ->
              let open Hitscore_db.Record_sample in
              let { name; _ } = get_fields (cache_value ~dbh sample) in
              fprintf o "{c|%s}" name;
            end;
            let people =
              let open Hitscore_db.Record_person in
              Array.map contacts ~f:(fun p ->
                let { print_name; email; _ } = get_fields (cache_value ~dbh p) in
                sprintf "%s <%s>"
                  (Option.value ~default:"{i|NO NAME}" print_name)
                  (Option.value ~default:"{i|NO-EMAIL}" email)) |> Array.to_list in
            fprintf o "{c|%s}\n"
              (String.concat ~sep:", " people)
          );
        );
        fprintf o "{end}\n";
      );
    )

  | _ -> ()
