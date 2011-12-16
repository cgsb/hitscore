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
  printf "Person: %s %s %s\n" first last email;
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

let parse hsc file =
  match db_connect hsc with
  | Ok dbh ->
    let print = ksprintf in
    let errbuf = Buffer.create 42 in
    let error fmt =
      let  f = Buffer.add_string errbuf in
      print f ("ERROR:" ^^ fmt ^^ "\n") in
    let warning fmt =
      let  f = Buffer.add_string errbuf in
      print f ("WARNING:" ^^ fmt ^^ "\n") in
    let strlist l = String.concat ~sep:"; " (List.map l (sprintf "%S")) in
    let loaded = Csv.load ~separator:',' file |> Array.of_list in
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
      match loaded.(contacts + 1) with
      | "" :: [] | [] -> warning "No contacts."
      | "" :: by3 -> check_contact by3
      | l -> error "Wrong contact line: %s" (strlist l)
    in
    let () = 
      match loaded.(charge_to_ + 1) with
      | "" :: first :: last :: email :: rest ->
        printf "PI: ";
        find_contact ~dbh first last email;
        printf "Don't know what to do with: [%s]\n" (strlist rest)
      | l -> error "Wrong investigator line: %s" (strlist l)
    in
    printf "ERRORS and WARNINGS:\n%s\n" (Buffer.contents errbuf);
    ()
  | Error (`pg_exn e) ->
    eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
