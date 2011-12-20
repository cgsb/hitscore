
open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
open Hitscore_threaded

let v011_to_v02 file_in file_out =
  let module V011 = V011.Make(Result_IO) in
  let module V02 = V02.Make(Result_IO) in
  let dump_v011 = In_channel.(with_file file_in ~f:input_all) in
  let s011 = V011.dump_of_sexp Sexplib.Sexp.(of_string dump_v011) in

  printf "s011 version: %s\n" s011.V011.version;


  let d02 = {
    V02.version = "0.2";
    file_system = s011.V011.file_system;
    record_log = s011.V011.record_log;
    record_person = s011.V011.record_person;
    record_organism = s011.V011.record_organism;
    record_sample = s011.V011.record_sample;
    record_protocol = s011.V011.record_protocol;
    record_custom_barcode = [];
    record_stock_library = (* TODO s011.V011.record_stock_library *) [];
    record_key_value = s011.V011.record_key_value;
    record_input_library = (* s011.V011.record_input_library *) [];
    record_lane = (* s011.V011.record_lane *) [];
    record_flowcell = s011.V011.record_flowcell;
    record_invoicing = [];
    record_bioanalyzer = [];
    record_agarose_gel = [];
    record_hiseq_raw = [];
    record_inaccessible_hiseq_raw = [];
    record_sample_sheet = [];
    function_assemble_sample_sheet = [];

    record_bcl_to_fastq_unaligned = [];
    function_bcl_to_fastq = [];
    function_transfer_hisqeq_raw = [];

    function_delete_intensities = [];

    record_hiseq_checksum = [];
    function_dircmp_raw = [];
    record_client_fastqs_dir = [];
    function_prepare_delivery = [];
  } in

  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum (V02.sexp_of_dump d02))));

  ()

let () =
  match Array.to_list Sys.argv with
  | exec :: "v011-v02" :: file_in :: file_out :: [] ->
    v011_to_v02 file_in file_out 
  | _ ->
    eprintf "usage: %s {v011-v02} <dump-in> <dump-out>\n" Sys.argv.(0)
