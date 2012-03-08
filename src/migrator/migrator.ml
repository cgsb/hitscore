
open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
open Hitscore_threaded

let v04_to_v05 file_in file_out =
  let module V04M = V04.Make(Result_IO) in
  let module V05M = V05.Make(Result_IO) in
  let dump_v04 = In_channel.(with_file file_in ~f:input_all) in
  let s04 = V04M.dump_of_sexp Sexplib.Sexp.(of_string dump_v04) in

  let option_map = Option.map in
  let open V05M in

  let file_system = 
    List.map s04.V04M.file_system (fun cache ->
      let (_volume_entry_cache, _file_cache_list) = cache in

      let files =
(* type _file_cache = (int32 * string * string * int32 array) 
type file_entry = {f_id : int32; f_name : string;
f_type : Hitscore_db_access.Mapke.Enumeration_file_type.t;
f_content : int32 array;} *)
        List.map _file_cache_list (fun (f_id, f_name, s_type, f_content) ->
          {V05M.File_system.f_id; f_name;
           f_type = V05M.Enumeration_file_type.of_string s_type
                    |! Result.ok_exn ~fail:(Failure "file-type of string");
           f_content})
      in
      let volume_entry =
        let v_id, v_toplevel, v_hr_tag, v_content = _volume_entry_cache in
        {V05M.File_system. v_id; v_toplevel; v_hr_tag; v_content}
      in
      {V05M.File_system. volume_entry; files}
(* type _volume_entry_cache = 
(int32 * string * string option * int32 array) with sexp
  type volume_entry = {v_id : int32; v_toplevel : string;
  	v_hr_tag : string option; v_content : int32 array;} *)
    ) in
  
  let record_log                     =
    List.map s04.V04M.record_log (fun cache -> 
      (* (int32 * string option * string option * string) *)
      let g_id, created, last_modified, log = cache in
      {V05M.Record_log.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       log;}) in
  let record_person                  =
    List.map s04.V04M.record_person (fun cache -> 
      (* (int32 * string option * string option * string option * string *
         string option * string * string * string * string option * string
         option * string * string option) with sexp *)

      let g_id, created, last_modified, print_name, given_name,
      middle_name, family_name, email, secondary_emails, login,
      nickname, roles, note = cache in
      {V05M.Record_person.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       print_name;
       given_name;
       middle_name;
       family_name;
       email;
       secondary_emails = Array.t_of_sexp String.t_of_sexp
          (Sexplib.Sexp.of_string secondary_emails);
       login;
       nickname;
       roles = Array.t_of_sexp V05M.Enumeration_role.t_of_sexp
          (Sexplib.Sexp.of_string roles);
       note;}) in
  let record_organism                =
    List.map s04.V04M.record_organism (fun cache ->
      let (g_id, created, last_modified, name, informal, note) = cache in
      {V05M.Record_organism.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       name = name;
       informal = informal;
       note = note;
      }) in
  let record_sample                  =
     List.map s04.V04M.record_sample (fun cache -> 
      let (g_id, created, last_modified, 
           name, project, organism, note) = cache in
      {V05M.Record_sample.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       name = name;
       project = project;
       organism = (option_map organism (fun id -> { Record_organism.id }));
       note = note;
      }) in
  let record_protocol                =
    List.map s04.V04M.record_protocol (fun cache ->
      let (g_id, created, last_modified, 
           name, doc, note) = cache in
      {V05M.Record_protocol.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       
       name = name;
       doc = { File_system.id = doc } ;
       note = note;
      })in
  let record_custom_barcode          =
    List.map s04.V04M.record_custom_barcode (fun cache -> 
      let (g_id, created, last_modified, 
position_in_r1, position_in_r2, position_in_index, sequence) = cache in
        {V05M.Record_custom_barcode.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    position_in_r1 = position_in_r1;
    position_in_r2 = position_in_r2;
    position_in_index = position_in_index;
    sequence = sequence;
  }) in

  let array_map a f = Array.map a ~f in

  let record_stock_library          =
    List.map s04.V04M.record_stock_library (fun cache -> 
      let (g_id, created, last_modified, name, project, description,
      sample, protocol, application, stranded, truseq_control,
      rnaseq_control, barcode_type, barcodes, custom_barcodes,
      p5_adapter_length, p7_adapter_length, preparator, note) = cache
      in
        {V05M.Record_stock_library.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    name = name;
    project = project;
    description = description;
    sample = (option_map sample (fun id -> { Record_sample.id }));
    protocol = (option_map protocol (fun id -> { Record_protocol.id }));
    application = application;
    stranded = stranded;
    truseq_control = truseq_control;
    rnaseq_control = rnaseq_control;
    barcode_type = (Enumeration_barcode_provider.of_string_exn barcode_type);
    barcodes = barcodes;
    custom_barcodes = (array_map custom_barcodes (fun id ->  { Record_custom_barcode.id }));
    p5_adapter_length = p5_adapter_length;
    p7_adapter_length = p7_adapter_length;
    preparator = (option_map preparator (fun id -> { Record_person.id }));
    note = note;
  })
  in
  let record_key_value               =
    List.map s04.V04M.record_key_value (fun cache -> 
      let (g_id, created, last_modified, key, value) = cache in
      {V05M.Record_key_value.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       
       key = key;
       value = value;
      })
  in
  let record_input_library           =
    List.map s04.V04M.record_input_library (fun cache -> 
      let (g_id, created, last_modified, library, submission_date,
      volume_uL, concentration_nM, user_db, note) = cache in
      {V05M.Record_input_library.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    library = { Record_stock_library.id = library };
    submission_date = (Timestamp.of_string submission_date);
    volume_uL = volume_uL;
    concentration_nM = concentration_nM;
    user_db = (array_map user_db (fun id ->  { Record_key_value.id }));
    note = note;
      })
  in
  let record_lane                    =
    List.map s04.V04M.record_lane (fun cache -> 
      let (g_id, created, last_modified, seeding_concentration_pM,
      total_volume, libraries, pooled_percentages,
      requested_read_length_1, requested_read_length_2, contacts) =
      cache in
        {V05M.Record_lane.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    seeding_concentration_pM = seeding_concentration_pM;
    total_volume = total_volume;
    libraries = (array_map libraries (fun id ->  { Record_input_library.id }));
    pooled_percentages = Core.Std.(Array.t_of_sexp Float.t_of_sexp (Sexplib.Sexp.of_string pooled_percentages));
    requested_read_length_1 = requested_read_length_1;
    requested_read_length_2 = requested_read_length_2;
    contacts = (array_map contacts (fun id ->  { Record_person.id }));
  })
  in
  let record_flowcell                =
    List.map s04.V04M.record_flowcell (fun cache -> 
      let (g_id, created, last_modified, serial_name, lanes) = cache in
        {V05M.Record_flowcell.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    serial_name = serial_name;
    lanes = (array_map lanes (fun id ->  { Record_lane.id }));
  })
  in
  let record_invoicing               =
    List.map s04.V04M.record_invoicing (fun cache -> 
      let (g_id, created, last_modified, pi, account_number, fund, org, program, project, lanes, percentage, note) = cache in
        {V05M.Record_invoicing.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    pi = { Record_person.id = pi };
    account_number = account_number;
    fund = fund;
    org = org;
    program = program;
    project = project;
    lanes = (array_map lanes (fun id ->  { Record_lane.id }));
    percentage = percentage;
    note = note;
  })
  in
  let record_bioanalyzer             =
    List.map s04.V04M.record_bioanalyzer (fun cache -> 
      let (g_id, created, last_modified, library, well_number,
           mean_fragment_size, min_fragment_size, max_fragment_size, note,
           files) = cache in
      {V05M.Record_bioanalyzer.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       
    library = { Record_stock_library.id = library };
    well_number = well_number;
    mean_fragment_size = mean_fragment_size;
    min_fragment_size = min_fragment_size;
    max_fragment_size = max_fragment_size;
    note = note;
    files = (option_map files (fun id -> { File_system.id }));
        })  in
  let record_agarose_gel             =
    List.map s04.V04M.record_agarose_gel (fun cache -> 
      let (g_id, created, last_modified, library, well_number, mean_fragment_size, min_fragment_size, max_fragment_size, note, files) = cache in
        {V05M.Record_agarose_gel.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    library = { Record_stock_library.id = library };
    well_number = well_number;
    mean_fragment_size = mean_fragment_size;
    min_fragment_size = min_fragment_size;
    max_fragment_size = max_fragment_size;
    note = note;
    files = (option_map files (fun id -> { File_system.id }));
  })
  in
  let record_hiseq_raw               =
    List.map s04.V04M.record_hiseq_raw (fun cache -> 
      let (g_id, created, last_modified, flowcell_name, read_length_1, read_length_index, read_length_2, with_intensities, run_date, host, hiseq_dir_name) = cache in
        {V05M.Record_hiseq_raw.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    flowcell_name = flowcell_name;
    read_length_1 = read_length_1;
    read_length_index = read_length_index;
    read_length_2 = read_length_2;
    with_intensities = with_intensities;
    run_date = (Timestamp.of_string run_date);
    host = host;
    hiseq_dir_name = hiseq_dir_name;
  })
  in
  let record_inaccessible_hiseq_raw  =
    List.map s04.V04M.record_inaccessible_hiseq_raw (fun cache -> 
      let (g_id, created, last_modified, deleted) = cache in
        {V05M.Record_inaccessible_hiseq_raw.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    deleted = (array_map deleted (fun id ->  { Record_hiseq_raw.id }));
  })
  in
  let record_sample_sheet            =
    List.map s04.V04M.record_sample_sheet (fun cache ->
      let (g_id, created, last_modified, file, note) = cache in
        {V05M.Record_sample_sheet.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    file = { File_system.id = file } ;
    note = note;
  })
  in
  let function_assemble_sample_sheet =
    List.map s04.V04M.function_assemble_sample_sheet (fun cache -> 
      let (g_id, result, g_recomputable, g_recompute_penalty,
        g_inserted, g_started, g_completed, g_status,
        kind, flowcell) = cache in
        {V05M.Function_assemble_sample_sheet.
  	 g_id;
  	 g_result = option_map result (fun id -> {Record_sample_sheet.id});
  	 g_recomputable;
  	 g_recompute_penalty;
  	 g_inserted = Option.map g_inserted Time.of_string;
  	 g_started = Option.map g_started Time.of_string;
  	 g_completed = Option.map g_completed Time.of_string;
  	 g_status = Enumeration_process_status.of_string_exn g_status;
         kind = (Enumeration_sample_sheet_kind.of_string_exn kind);
         flowcell = { Record_flowcell.id = flowcell };
        })
  in
  let record_bcl_to_fastq_unaligned  =
    List.map s04.V04M.record_bcl_to_fastq_unaligned (fun cache -> 
      let (g_id, created, last_modified, directory) = cache in
        {V05M.Record_bcl_to_fastq_unaligned.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;

    directory = { File_system.id = directory } ;
  })
  in
  let function_bcl_to_fastq          =
    List.map s04.V04M.function_bcl_to_fastq (fun cache -> 
      let (g_id, result, g_recomputable, g_recompute_penalty,
           g_inserted, g_started, g_completed, g_status,
           raw_data, availability, mismatch, version, tiles, sample_sheet) = cache in
        {V05M.Function_bcl_to_fastq.
  	 g_id;
  	 g_result = option_map result (fun id -> {Record_bcl_to_fastq_unaligned.id});
  	 g_recomputable;
  	 g_recompute_penalty;
  	 g_inserted = Option.map g_inserted Time.of_string;
  	 g_started = Option.map g_started Time.of_string;
  	 g_completed = Option.map g_completed Time.of_string;
  	 g_status = Enumeration_process_status.of_string_exn g_status;
         raw_data = { Record_hiseq_raw.id = raw_data };
         availability = { Record_inaccessible_hiseq_raw.id = availability };
         mismatch = mismatch;
         version = version;
         tiles = tiles;
         sample_sheet = { Record_sample_sheet.id = sample_sheet };
        })
  in
  let function_transfer_hisqeq_raw   =
    List.map s04.V04M.function_transfer_hisqeq_raw (fun cache -> 
      let (g_id, result, g_recomputable, g_recompute_penalty,
        g_inserted, g_started, g_completed, g_status,
           hiseq_raw, availability, dest) = cache in
        {V05M.Function_transfer_hisqeq_raw.
  	 g_id;
  	 g_result = option_map result (fun id -> {Record_hiseq_raw.id});
  	 g_recomputable;
  	 g_recompute_penalty;
  	 g_inserted = Option.map g_inserted Time.of_string;
  	 g_started = Option.map g_started Time.of_string;
  	 g_completed = Option.map g_completed Time.of_string;
  	 g_status = Enumeration_process_status.of_string_exn g_status;
         hiseq_raw = { Record_hiseq_raw.id = hiseq_raw };
         availability = { Record_inaccessible_hiseq_raw.id = availability };
         dest = dest;
        })
  in
  let function_delete_intensities    =
    List.map s04.V04M.function_delete_intensities (fun cache -> 
      let (g_id, result, g_recomputable, g_recompute_penalty,
        g_inserted, g_started, g_completed, g_status,
        hiseq_raw, availability) = cache in
      {V05M.Function_delete_intensities.
       g_id;
  	 g_result = option_map result (fun id -> {Record_hiseq_raw.id});
  	 g_recomputable;
  	 g_recompute_penalty;
  	 g_inserted = Option.map g_inserted Time.of_string;
  	 g_started = Option.map g_started Time.of_string;
  	 g_completed = Option.map g_completed Time.of_string;
  	 g_status = Enumeration_process_status.of_string_exn g_status;
    hiseq_raw = { Record_hiseq_raw.id = hiseq_raw };
    availability = { Record_inaccessible_hiseq_raw.id = availability };
        })
  in
  let record_hiseq_checksum          =
    List.map s04.V04M.record_hiseq_checksum (fun cache -> 
      let (g_id, created, last_modified, file) = cache in
        {V05M.Record_hiseq_checksum.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       file = { File_system.id = file } ;
        })
  in
  let function_dircmp_raw            =
    List.map s04.V04M.function_dircmp_raw (fun cache -> 
      let (g_id, result, g_recomputable, g_recompute_penalty,
        g_inserted, g_started, g_completed, g_status,
        hiseq_raw, availability) = cache in
      {V05M.Function_dircmp_raw.
       g_id;
  	 g_result = option_map result (fun id -> {Record_hiseq_checksum.id});
  	 g_recomputable;
  	 g_recompute_penalty;
  	 g_inserted = Option.map g_inserted Time.of_string;
  	 g_started = Option.map g_started Time.of_string;
  	 g_completed = Option.map g_completed Time.of_string;
  	 g_status = Enumeration_process_status.of_string_exn g_status;
    hiseq_raw = { Record_hiseq_raw.id = hiseq_raw };
    availability = { Record_inaccessible_hiseq_raw.id = availability };
        })

  in
  let record_client_fastqs_dir       =
    List.map s04.V04M.record_client_fastqs_dir (fun cache -> 
      let (g_id, created, last_modified,dir__dirfile) = cache in
      {V05M.Record_client_fastqs_dir.
       g_id;
       g_created = Option.map created Time.of_string;
       g_last_modified = Option.map created Time.of_string;
       
       dir = dir__dirfile;
      }) in
  let function_prepare_unaligned_delivery =
    assert (s04.V04M.function_prepare_delivery = []); []
  in

  let d05 =
    let open V05M in
    {
      version = V05.Info.version;
      file_system                    ;
      record_log                     ;
      record_person                  ;
      record_organism                ;
      record_sample                  ;
      record_protocol                ;
      record_custom_barcode          ;
      record_stock_library           ;
      record_key_value               ;
      record_input_library           ;
      record_lane                    ;
      record_flowcell                ;
      record_invoicing               ;
      record_bioanalyzer             ;
      record_agarose_gel             ;
      record_hiseq_raw               ;
      record_inaccessible_hiseq_raw  ;
      record_sample_sheet            ;
      function_assemble_sample_sheet ;
      record_bcl_to_fastq_unaligned  ;
      function_bcl_to_fastq          ;
      function_transfer_hisqeq_raw   ;
      function_delete_intensities    ;
      record_hiseq_checksum          ;
      function_dircmp_raw            ;
      record_client_fastqs_dir       ;
      function_prepare_unaligned_delivery;
    } in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum (V05M.sexp_of_dump d05))));

  ()


let v05_to_v051 file_in file_out =
  let dump_v05 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () =
    let module V05M = V05.Make (Result_IO) in
    V05M.dump_of_sexp dump_v05 |! ignore in

  let dump_v051 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom "0.5"] -> List [Atom "version"; Atom "0.5.1"] 
      | List [Atom "hiseq_dir_name"; Atom name] ->
        List [Atom "hiseq_dir_name"; Atom (Filename.basename name)]
      | List l -> List (List.map ~f:parse l) in
    parse dump_v05 in
  
  let () =
    let module V05M1 = V051.Make (Result_IO) in
    V05M1.dump_of_sexp dump_v051 |! ignore in
  
  let module V051M = V051.Make(Result_IO) in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v051)));
  ()


let v051_to_v06 file_in file_out =
  let dump_v051 =
    In_channel.(with_file file_in ~f:input_all) |! Sexplib.Sexp.of_string in

  let () =
    let module V051M = V051.Make (Result_IO) in
    V051M.dump_of_sexp dump_v051 |! ignore in

  let dump_v06 =
    let open Sexplib.Sexp in
    let rec parse =
      function
      | Atom a -> Atom a
      | List [Atom "version"; Atom old_version]
          when old_version = V051.Info.version ->
        printf "%s Vs %s\n" old_version V051.Info.version;
        List [Atom "version"; Atom V06.Info.version ] 
      | List l -> List (List.map ~f:parse l) in
    parse dump_v051 in
  
  let () =
    let module V05M1 = V06.Make (Result_IO) in
    V05M1.dump_of_sexp dump_v06 |! ignore in
  
  let module V06M = V06.Make(Result_IO) in
  Out_channel.(with_file file_out ~f:(fun o ->
    output_string o (Sexplib.Sexp.to_string_hum dump_v06)));
  ()
  
let () =
  match Array.to_list Sys.argv with
  | exec :: "v04-v05" :: file_in :: file_out :: [] ->
    v04_to_v05 file_in file_out 
  | exec :: "v05-v051" :: file_in :: file_out :: [] ->
    v05_to_v051 file_in file_out 
  | exec :: "v051-v06" :: file_in :: file_out :: [] ->
    v051_to_v06 file_in file_out 
  | _ ->
    eprintf "usage: %s {v04-v05,v05-v051,v051-v06} <dump-in> <dump-out>\n"
      Sys.argv.(0)
