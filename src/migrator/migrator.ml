
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

  let check_barcode_provider bc =
    V02.Enumeration_barcode_provider.of_string bc |> 
        Result.ok_exn ~fail:(Failure "barcode provider is wrong") |>
            V02.Enumeration_barcode_provider.to_string
  in


  let record_lane, record_input_library, record_stock_library =
    let sl_rl1_rl2 = ref [] in
    let stocks =
      List.map s011.V011.record_stock_library 
        (fun cache ->
          let (g_id, g_created, g_last_modified, name, sample,
               protocol, application, stranded, truseq_control,
               rnaseq_control, read_length_1, read_length_2, barcode_type,
               barcodes, note) = cache in
   
          sl_rl1_rl2 := (g_id, read_length_1, read_length_2) :: !sl_rl1_rl2;

          let project = None in
          let custom_barcodes = [| |] in
          let p5_adapter_length = None in
          let p7_adapter_length = None in
          let preparator = None in
          (g_id, g_created, g_last_modified, 
           name, project, sample,
           protocol, application, stranded, truseq_control,
           rnaseq_control,
           check_barcode_provider barcode_type, 
           barcodes, custom_barcodes,
           p5_adapter_length, p7_adapter_length, preparator, note)
        )
    in
    let il_sl_contacts = ref [] in
    let inputs =
      List.map s011.V011.record_input_library (fun cache ->
        let (g_id, g_created, g_last_modified, library,
             submission_date, contacts, volume_uL, concentration_nM,
             user_db, note) = cache in

        il_sl_contacts := (g_id, library, contacts) :: !il_sl_contacts;

        (g_id, g_created, g_last_modified, library, submission_date,
         volume_uL, concentration_nM, user_db, note)) in

    let lanes =
      List.map s011.V011.record_lane (fun cache ->
        let (g_id, g_created, g_last_modified, seeding_concentration,
             total_volume, libraries, pooled_percentages) = cache in

        let seeding_concentration_pM = seeding_concentration in

        let contacts =
          let redundant_contacts =
            List.find_all !il_sl_contacts ~f:(fun (il, sl, cs) ->
              List.exists (Array.to_list libraries) ~f:((=) il))
          in
          let check_uniformity x =
            List.reduce x ~f:(fun (il1, sl1, ca1) (il2, sl2, ca2) ->
              let cmp = compare in
              if il1 = 149l (* PhiX_v2 *) ||
                il1 = 150l (* PhiX_v3 *) ||
                Array.sort ~cmp ca1 = Array.sort ~cmp ca2 then (il2, sl2, ca2)
              else (
                List.iter redundant_contacts (fun (il, sl, ca) -> 
                  printf "[%ld] " il;
                  Array.iter ca (fun c ->
                    printf "%ld, " c);
                  printf "\n"); 
                failwith "Contact list is not uniform !!")) in
          match check_uniformity redundant_contacts with
          | Some  (il, sl, ca) -> ca
          | None -> failwith "a lane with no libraries???"
        in

        let requested_read_length_1, requested_read_length_2 =
          let stock_libs = 
            List.find_all !il_sl_contacts ~f:(fun (il, sl, cs) ->
              List.exists (Array.to_list libraries) ~f:((=) il)) |>
                List.map ~f:(fun (il, sl, ca) -> sl) |>
                    List.dedup in
          (* printf "SLs: "; *)
          (* List.iter stock_libs (printf "%ld, "); *)
          let read_lengths = 
            List.find_all !sl_rl1_rl2 ~f:(fun (sl, rl1, rl2) ->
              sl <> 149l (* PhiX_v2 *) 
              && sl <> 150l (* PhiX_v3 *)
            && List.exists stock_libs ~f:((=) sl)) |>
                List.map ~f:(fun (sl, rl1, rl2) -> (rl1, rl2)) |>
                    List.dedup
          in
          match read_lengths with
          | [] -> (* Should be a PhiX *)
            let flowcell_lanes =
              List.find_map s011.V011.record_flowcell ~f:(fun cache ->
                let (_, g_created, g_last_modified, serial_name, lanes) = cache in
                if Array.mem g_id lanes then Some lanes else None) |>
                  function
                  | Some s -> Array.to_list s
                  | None -> failwith "Can't find in which flowcell the lane is"
            in
            let sister_libs =
              List.find_map flowcell_lanes (fun l ->
                List.find_map s011.V011.record_lane ~f:(fun cache ->
                  let (id, g_created, g_last_modified, seeding_concentration,
                       total_volume, libraries, pooled_percentages) = cache in
                  if id = l && g_id <> id then
                    Some (Array.to_list libraries) else None)) |>
                  function
                  | Some s -> s
                  | None -> failwith "Cannot find any other library ??"
            in
            let sister_lib_read_length =
              (* We assume input_lib.id = stock_lib.id *)
              List.find_map s011.V011.record_stock_library ~f:(fun cache ->
                let (id, g_created, g_last_modified, name, sample,
                     protocol, application, stranded, truseq_control,
                     rnaseq_control, read_length_1, read_length_2, barcode_type,
                     barcodes, note) = cache in
                if id <> 149l && id <> 150l &&
                  List.exists sister_libs ((=) id)
                then Some (read_length_1, read_length_2)
                  else None) |>
                  function Some s -> s 
                  | None -> failwith "can't find sister read length"
            in
            sister_lib_read_length
          | [ one_rl1, Some one_rl2 ] -> (one_rl1, Some one_rl2)
          | l ->
            printf "Don't know what to do:\n";
            List.iteri l ~f:(fun i (rl1, rl2) ->
              printf "(%d: %ld ...) " i rl1);
            printf "\n";
            failwith "wrong requested_read_lengths"

        in

        let pooled_percentages_floats_sexp =
          Array.map pooled_percentages ~f:(fun i32 ->
            i32 |! Int64.of_int32 |! Float.of_int64)
          |! Array.sexp_of_t Float.sexp_of_t |! Sexp.to_string in

        (g_id, g_created, g_last_modified, seeding_concentration_pM,
        total_volume, libraries, pooled_percentages_floats_sexp,
        requested_read_length_1, requested_read_length_2, contacts))
    in

    lanes, inputs, stocks in

  let record_sample = 
    List.map s011.V011.record_sample (fun cache ->
      let (g_id, g_created, g_last_modified, name, organism, note) = cache in
      (g_id, g_created, g_last_modified, name, 
       (None : string option), organism, note))
  in
  let d02 = {
    V02.version = "0.2-dev";
    file_system = s011.V011.file_system;
    record_log = s011.V011.record_log;
    record_person = s011.V011.record_person;
    record_organism = s011.V011.record_organism;
    record_sample;
    record_protocol = s011.V011.record_protocol;
    record_custom_barcode = [];
    record_stock_library;
    record_key_value = s011.V011.record_key_value;
    record_input_library;
    record_lane;
    record_flowcell = s011.V011.record_flowcell;
    record_invoicing = [];
    record_bioanalyzer = s011.V011.record_bioanalyzer;
    record_agarose_gel = s011.V011.record_agarose_gel;
    record_hiseq_raw = s011.V011.record_hiseq_raw;
    record_inaccessible_hiseq_raw = s011.V011.record_inaccessible_hiseq_raw;
    record_sample_sheet = s011.V011.record_sample_sheet;
    function_assemble_sample_sheet = s011.V011.function_assemble_sample_sheet;

    record_bcl_to_fastq_unaligned = s011.V011.record_bcl_to_fastq_unaligned;
    function_bcl_to_fastq = s011.V011.function_bcl_to_fastq;
    function_transfer_hisqeq_raw = s011.V011.function_transfer_hisqeq_raw;

    function_delete_intensities = s011.V011.function_delete_intensities;

    record_hiseq_checksum = s011.V011.record_hiseq_checksum;
    function_dircmp_raw = s011.V011.function_dircmp_raw;
    record_client_fastqs_dir = s011.V011.record_client_fastqs_dir;
    function_prepare_delivery = s011.V011.function_prepare_delivery;
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
