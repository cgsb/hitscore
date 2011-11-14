#! /usr/bin/env ocamlscript
Ocaml.ocamlflags := ["-thread"];
Ocaml.packs := ["hitscore"; "csv"]
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

      
let main metadata_prefix dbh =

  let bioo_barcodes_db = 
    List.map bioo_barcodes (fun (vendor_id, barcode) ->
      (Hitscore_db_access.Record_bioo_barcode.add_value dbh
         ~vendor_id:(Int32.of_int_exn vendor_id)
         ~barcode, vendor_id, barcode))
  in
  let truseq_barcodes_db = 
    List.map illumina_barcodes (fun (vendor_id, barcode) ->
      (Hitscore_db_access.Record_truseq_barcode.add_value dbh
         ~vendor_id:(Int32.of_int_exn vendor_id)
         ~barcode, vendor_id, barcode))
  in
  (* let mkpath = List.fold_left ~f:Filename.concat ~init:metadata_dir in *)
  let load = Csv.load ~separator:',' in
  let load_table t =
    load (sprintf "%s%s.csv" metadata_prefix t) |> Csv.square |> List.tl_exn in
  let persons =
    let tbl = load_table "Person" in
    List.map tbl ~f:(function
      | [_;first;last;email] ->
        let id = 
          Hitscore_db_access.Record_person.add_value
            ~print_name:(sprintf "%s %s" first last)
            ~family_name:last
            ~email dbh in
        (id, first, last, email)
      | _ -> failwith "persons") in
(*
  let bcl2fastqs = 
    let tbl = load_table "BclToFastq" in
    List.map tbl (function
      | [ _; fcid; mm; note; version ] ->
        let id = 
          Hitscore_db_access.Function_bcl_to_fastq.add_evaluation
            ~raw_data:(* TODO *)
            ~version
            ~mismatch:(Int32.of_int mm)
            ~sample_sheet:(* TODO *)
            ~recomputable:true
            ~recompute_penalty:10_000.
            dbh in
        (id, fcid, mm, )

      | _ -> failwith "BclToFastq"
*)
  let libraries =
    let unknown_organism =
      Hitscore_db_access.Record_organism.add_value
        ~name:"UNKNOWN"
        ~note:"Legacy organism; from old libraries which \
               did not set that field"
        dbh
    in
    let legacy_protocol =
      Hitscore_db_access.Record_protocol.add_value dbh
        ~name:"Legacy Protocol"
        ~path:"/not/found" ~size:0l ~filetype:"EMPTY" in
    let no_name_person =
      Hitscore_db_access.Record_person.add_value
        ~print_name:"Print NAME"
        ~family_name:"NAME"
        ~email:"pn000@nyu.edu" dbh ~note:"MADE UP >>> TO FIX !!!" in

    let organisms = ref [ ("", unknown_organism) ] in
    let tbl = load_table "Library" in
    List.map tbl (function
      | [ _; vial_id; lib_id; sample_id1; sample_id2; organism_name; 
          investigator_name; application; stranded; control_type;
          truseq_control_used; rna_seq_Control;
          truseq_barcode; bioo_Barcode;
          library_submitted; library_QuantificationMethod;
          libraryconc_nM_LAB; libraryVolumeuL;
          fragmentSizeQCMethod; pdfPath; wellNumber; avgLibraryFragmentSize;
          fragmentSizeQCMethod2; pdfPath2; wellNumber2; avgLibraryFragmentSize2;
          readType; readLength; note ] ->

        (* eprintf "Lib: %s\n" lib_id; *)
        let organism =  
          match List.find !organisms (fun (n, _) -> n = organism_name) with
          | Some (_, id) -> id
          | None ->
            let id = 
              Hitscore_db_access.Record_organism.add_value
                ~name:organism_name dbh in
            organisms := (organism_name, id) :: !organisms;
            id
        in
        let sample, sample_name =
          let name, note =
            if sample_id1 = "" then
              (lib_id ^ "sample", Some "Sample name made from lib_id")
            else
              (sample_id1, None)
          in
          (Hitscore_db_access.Record_sample.add_value dbh
             ~name ~organism ?note, name)
        in
        let protocol = legacy_protocol in
        let investigator = 
          match List.find persons 
            (fun (_, p, l, e) -> l = investigator_name) with
            | Some (id, _, _, _) -> id
            | None -> no_name_person
        (* sprintf "Unknown Investigator: %s" investigator_name |> failwith *)
        in
        let library_prep = 
          Hitscore_db_access.Function_library_preparation.add_evaluation dbh
            ~sample ~protocol
            ~investigator
            ~contacts:[| |]
            ~vial_id:999999999l (* Int32.of_string vial_id *)
            ~name:(if lib_id = "" then "lib_of_sample_" ^ sample_name else lib_id)
            ~application
            ~stranded:false
            ~control_type
            ~truseq_control:(truseq_control_used = "TRUE")
            ~rnaseq_control:rna_seq_Control
            ~read_type:(
              match readType, readLength with
              | "PE", "50x50" -> `read_PE_50x50
              | "SE", "50x50" -> `read_SE_50
              | "PE", "100x100" -> `read_PE_100x100
              | "SE", "100x100" -> `read_SE_100
              | _, _ -> 
                sprintf "Can't understand read_type/length of %s, %s"
                  lib_id sample_id1 |> failwith)
            ~recomputable:false
        in
        let is_qpcred =
          match library_QuantificationMethod with
          | "qPCR" | "qPCR " -> true
          | _ -> false in
        let library_result = 
          let bioo_barcodes =
            List.filter_map bioo_barcodes_db 
              (fun (handle, nb, bc) ->
                let snb =  string_of_int nb in
                if snb = bioo_Barcode then Some handle else None) |> Array.of_list in
          let truseq_barcodes =
            List.filter_map truseq_barcodes_db 
              (fun (handle, nb, bc) ->
                let snb =  string_of_int nb in
                if snb = truseq_barcode || sprintf "AD%04d" nb = truseq_barcode then
                  Some handle else None) |> Array.of_list in
          let note =
            String.concat ~sep:"; " (List.flatten [
              ["Pre_db_library"];
              (if not is_qpcred then
                  [sprintf "No_qpcr(%S)" library_QuantificationMethod]
               else []);
              (if lib_id = "" then ["Lib_name_made_up"] else []);
            ])
          in
          Hitscore_db_access.Record_library.add_value dbh
            ~bioo_barcodes ~truseq_barcodes ~note
        in
        let library_prep_can_get_result =
          Hitscore_db_access.Function_library_preparation.set_succeeded
            library_prep dbh
            ~result:library_result in
        if is_qpcred then (
          let result =
            Hitscore_db_access.Record_qpcr_result.add_value dbh
              ~concentration:(float_of_string libraryconc_nM_LAB)
              ~volume:(float_of_string libraryVolumeuL) in 
          let func = 
            Hitscore_db_access.Function_qpcr.add_evaluation dbh
              ~library:library_result ~recomputable:false in
          Hitscore_db_access.Function_qpcr.set_succeeded 
            func dbh ~result |> ignore
        );
        
        library_prep_can_get_result

      | l ->
        sprintf "library: list of %d items: [%s]"
          (List.length l) (String.concat ~sep:", " l) |> failwith) in

  (persons, libraries) |> ignore

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
      all "organism";
      all "sample";
      all "library_preparation";
      all "library";
      all "qpcr";
      all "qpcr_result";
      "
select
  library_preparation.name as lib_name,
  library_preparation.read_type as read_type,
  library.note as lib_note,
  sample.name as sample_name,
  organism.name as org_name,
  protocol.name as protcol,
  library.bioo_barcodes as bioo_bc,
  library.truseq_barcodes as ts_bc,
  person.print_name as investigator
from library_preparation, sample, organism, protocol, library, person
where
library_preparation.sample = sample.g_id and
sample.organism = organism.g_id and
library_preparation.protocol = protocol.g_id and
library_preparation.g_result = library.g_id and
library_preparation.investigator = person.g_id

";
    ]
  | _ -> ()
