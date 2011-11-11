#! /usr/bin/env ocamlscript
Ocaml.ocamlflags := ["-thread"];
Ocaml.packs := ["hitscore"; "csv"]
--

open Core.Std
let (|>) x f = f x

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

      | l ->
        sprintf "library: list of %d items: [%s]"
          (List.length l) (String.concat ~sep:", " l) |> failwith) in

  (persons, libraries) |> ignore

let () =
  match Array.to_list Sys.argv with
  | exec :: dir :: [] -> 
    let dbh = PGOCaml.connect() in
    main dir dbh;
    Unix.system "
 rm ttt.html
 psql --html -c 'select * from person;' >> ttt.html
 psql --html -c 'select * from organism;' >> ttt.html
 psql --html -c 'select * from sample;' >> ttt.html
 psql --html -c 'select * from library_preparation;' >> ttt.html
 " |> ignore
  | _ -> ()
