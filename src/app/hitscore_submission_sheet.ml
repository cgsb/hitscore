open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)

let library_submission_date = 0 (* "Library Submission Date" *)
let number_of_lanes_required_ = 1 (* "Number of Lanes Required " *)
let contacts = 2 (* "Contacts" *)
let charge_to_ = 5 (* "Charge To:" *)
let library_identifier = 9 (* "Library Identifier" *)
let sample_identifier__library_protocol_input_ = 10 (* "Sample Identifier (Library Protocol Input)" *)
let application__dna_seq__rna_seq__chip_seq__etc__ = 11 (* "Application (DNA-seq, RNA-seq, ChIP-seq, etc.)" *)
let read_type_and_length__sr_50__sr_100__pe_50x50__or_pe_100x100__ = 12 (* "Read Type and Length (SR 50, SR 100, PE 50x50, or PE 100x100) " *)
let library_concentration__nm_ = 13 (* "Library Concentration (nM)" *)
let library_volume__ul_ = 14 (* "Library Volume (uL)" *)
let library_quantification_qpcr__required_ = 16 (* "Library Quantification qPCR (Required)" *)
let concentration__nm_ = 17 (* "Concentration (nM)" *)
let volume__ul_ = 18 (* "Volume (uL)" *)
let bioanalyzer_trace__required_ = 20 (* "Bioanalyzer Trace (Required)" *)
let bioanalyzer_pdf_file_name = 21 (* "Bioanalyzer PDF File Name" *)
let bioanalyzer_xad_file_name = 22 (* "Bioanalyzer XAD File Name" *)
let well_number = 23 (* "Well Number" *)
let average_library_fragment_size = 24 (* "Average Library Fragment Size" *)
let agarose_gel__optional_ = 26 (* "Agarose Gel (Optional)" *)
let agarose_gel_image_file = 27 (* "Agarose Gel Image File" *)
let well_number_ = 28 (* "Well Number " *)
let average_library_fragment_size = 29 (* "Average Library Fragment Size" *)
let illumina_barcode__if_used_ = 31 (* "Illumina Barcode (If Used)" *)
let bioo_barcode__if_used_ = 32 (* "BIOO Barcode (If Used)" *)
let species = 34 (* "Species" *)
let lane_group__libraries_to_be_run_on_the_same_lane_ = 35 (* "Lane Group (libraries to be run on the same lane)" *)
let truseq_controls__if_used_ = 36 (* "TruSeq Controls (If Used)" *)
let protocol_s__used = 37 (* "Protocol(s) used" *)
let notes = 38 (* "Notes" *)

open Hitscore_threaded 

let find_contact ~dbh first last email =
  printf "Person: %S %S %S\n" first last email;
  let by_email = Layout.Search.record_person_by_email ~dbh email in
  begin match by_email with
  | Ok [ one ] ->
    printf "  Found one person with that email: %ld.\n"
      one.Layout.Record_person.id;
  | Ok [] ->
    printf "  Found no person with that email … ";
    begin match Layout.Search.record_person_by_given_name_family_name
        ~dbh first last with
    | Ok [ one ] ->
      printf "BUT found one person with that name: %ld.\n"
        one.Layout.Record_person.id;
    | Ok [] ->
      printf "neither with that full name … ";
      begin match Layout.Search.record_person_by_nickname_family_name
              ~dbh first last with
      | Ok [ one ] ->
        printf "BUT found one person with that nick name: %ld.\n"
          one.Layout.Record_person.id;
      | Ok [] ->
        printf "neither with that nick-fullname … \n";
      | _ ->
        failwith "search by nick-full-name returned wrong"
      end
    | _ ->
      failwith "search by full-name returned wrong"
    end
  | _ ->
    failwith "search by email returned wrong"
  end

let virtual_cell i = function
  | [] | _ :: [] -> `none
  | [ one; two ] -> `one_for_all two
  | l -> 
    Option.value_map ~default:`out_of_bounds 
      (List.nth l i) ~f:(fun s -> `one_specific s)

let print = ksprintf
let errbuf = Buffer.create 42
let error fmt = 
  let  f = Buffer.add_string errbuf in
  print f ("ERROR:" ^^ fmt ^^ "\n")
let warning fmt =
  let  f = Buffer.add_string errbuf in
  print f ("WARNING:" ^^ fmt ^^ "\n")
let strlist l = String.concat ~sep:"; " (List.map l (sprintf "%S"))


let check_stuff ~row ~libnb ~libname ?search_and_get_id name =
  let option =
    match virtual_cell libnb row with
    | `none -> error "No %S provided: (%d, %s)" name libnb libname; None
    | `one_specific s ->
      printf "  Specific %s: %s" name s; Some s
    | `one_for_all s ->
      printf "  One-for-all %s: %s" name s; Some s
    | `out_of_bounds ->
      error "Wrong %s format (%d, %s)\n  [%s]" 
        name libnb libname (strlist row);
      None
  in
  let result =
    Option.map option (fun s ->
      (s,
       Option.bind search_and_get_id (fun (search, get_id) ->
         match search s with
         | Ok [ one ] ->
           printf ", found by name as %ld" (get_id one);
           (Some one)
         | Ok [] ->
           printf ", not found in the DB";
           (None)
         | _ ->
           failwithf "Layout.Search.record_sample_by_name (%s) was wrong" s ())))
  in
  printf ".\n";
  result

let parse hsc file =
  Buffer.clear errbuf;
  printf "========= Loading %S =========\n" file;
  match db_connect hsc with
  | Ok dbh ->
    let loaded = Csv.load ~separator:',' file |> Array.of_list in
    let sanitized =
      let sanitize s =
        (String.split_on_chars ~on:[' '; '\t'; '\n'; '\r'] s) |>
            List.filter ~f:((<>) "") |>
                String.concat ~sep:" "
      in
      Array.map loaded ~f:(List.map ~f:sanitize)
    in
    let () =
      let rec check_contact = function
        | first :: last :: email :: rest ->
          find_contact ~dbh first last email;
          check_contact rest
        | [] -> ()
        | l ->
          error "Wrong contacts format: not going by 3 (?):\n[%s]"
            (strlist l);
      in
      match sanitized.(contacts + 1) with
      | "" :: [] | [] -> warning "No contacts."
      | "" :: by3 -> check_contact by3
      | l -> error "Wrong contact line: %s" (strlist l)
    in
    let () = 
      match sanitized.(charge_to_ + 1) with
      | "" :: first :: last :: email :: rest ->
        printf "PI: ";
        find_contact ~dbh first last email;
        if rest <> [] then
          printf "Don't know what to do with: [%s]\n" (strlist rest)
      | l -> error "Wrong investigator line: %s" (strlist l)
    in
    List.iteri (List.tl_exn sanitized.(library_identifier)) (fun i libname ->
      let libnb = i + 1 in
      printf "Library %d : %s\n" libnb libname;
      let stock_library =
        check_stuff ~row:sanitized.(library_identifier) ~libnb ~libname
          ~search_and_get_id:
          ((fun s -> Layout.Search.record_stock_library_by_name ~dbh s),
           (fun one -> one.Layout.Record_stock_library.id))
          "stock library" in
      let species =
        check_stuff ~row:sanitized.(species) ~libnb ~libname
          ~search_and_get_id:
          ((fun s -> Layout.Search.record_organism_by_name ~dbh s),
           (fun one -> one.Layout.Record_organism.id))
          "species" in
      let sample =
        let row =
          sanitized.(sample_identifier__library_protocol_input_) in
        check_stuff ~row ~libnb ~libname
          ~search_and_get_id:
          ((fun s -> Layout.Search.record_sample_by_name ~dbh s),
           (fun one -> one.Layout.Record_sample.id))
          "sample"
      in
      let protocol =
        check_stuff ~row:sanitized.(protocol_s__used) ~libnb ~libname
          ~search_and_get_id:
          ((fun s -> Layout.Search.record_protocol_by_name ~dbh s),
           (fun one -> one.Layout.Record_protocol.id))
          "protocol"
      in

      let application =
        check_stuff ~row:sanitized.(application__dna_seq__rna_seq__chip_seq__etc__)
          ~libnb ~libname "application" in
      let int32 s = try Some (Int32.of_string s) with e ->
        error "Cannot read an integer from %s (%d, %s)" s libnb libname; None in
      let read_type =
        let checked =
          check_stuff 
            ~row:sanitized.(read_type_and_length__sr_50__sr_100__pe_50x50__or_pe_100x100__)
            ~libnb ~libname "read type" in
        Option.bind checked (fun (s, _) ->
          match String.split ~on:' ' s with
          | [ "PE"; lengths ] ->
            begin match String.split ~on:'x' lengths with
            | [ left; right ] ->
              Option.bind (int32 left) (fun l -> Some (s, l, int32 right))
            | _ ->
              error "Wrong read length spec (PE): %s (%d, %s)" s libnb libname;
              None
            end
          | [ "SE"; length ] ->
            Option.bind (int32 length) (fun l -> Some (s, l, None))
          | _ ->
            error "Wrong read length spec (PE): %s (%d, %s)" s libnb libname;
            None)
      in
      let lib_concentration =
        let open Option in
        check_stuff 
          ~row:sanitized.(library_concentration__nm_)
          ~libnb ~libname "library concentration" 
        >>= fun checked ->
        (int32 (fst checked)) in
      let lib_volume =
        let open Option in
        check_stuff 
          ~row:sanitized.(library_volume__ul_)
          ~libnb ~libname "library volume" 
        >>= fun checked ->
        (int32 (fst checked)) in

      ignore (species, sample, application, read_type, 
              lib_concentration, lib_volume,
              stock_library, protocol);
    );
    printf "ERRORS and WARNINGS:\n%s\n" (Buffer.contents errbuf);
    ()
  | Error (`pg_exn e) ->
    eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
