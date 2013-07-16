open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

module Hiseq_raw = Hitscore_hiseq_raw
module B2F_unaligned = Hitscore_b2f_unaligned

open Hitscore_data_access_types

module Xml_tree = struct
  include Xmlm
  let in_tree i =
    let el tag childs = `E (tag, childs)  in
    let data d = `D d in
    input_doc_tree ~el ~data i
end

module File_cache = struct

  module String_map = Core.Std.Map.Poly

  let _run_param_cache =
    ref (String_map.empty:
           (string,
            Hitscore_interfaces.Hiseq_raw_information.clusters_info option array)
           String_map.t)

  let get_clusters_info (path:string) =
    let make file =
      read_file file >>= fun xml_s ->
      let xml =
        Xml_tree.(in_tree (make_input (`String (0, xml_s)))) in
      Hiseq_raw.clusters_summary (snd xml) |! of_result
    in
    match String_map.find !_run_param_cache path with
    | Some r -> return r
    | None ->
      make path >>= fun res ->
      _run_param_cache := String_map.add ~key:path ~data:res !_run_param_cache;
      return res

  let _demux_summary_cache =
    ref (String_map.empty:
           (string,
            Hitscore_interfaces.B2F_unaligned_information.demux_summary) String_map.t)

  let get_demux_summary path =
    let make file =
      read_file file >>= fun xml_s ->
      let xml =
        Xml_tree.(in_tree (make_input (`String (0, xml_s)))) in
      B2F_unaligned.flowcell_demux_summary (snd xml)
      |! of_result
    in
    match String_map.find !_demux_summary_cache path with
    | Some r -> return r
    | None ->
      make path >>= fun res ->
      _demux_summary_cache :=
        String_map.add ~key:path ~data:res !_demux_summary_cache;
      return res

  let _fastx_quality_stats_cache =
    ref (String_map.empty: (string, fastx_quality_stats) String_map.t)

  let get_fastx_quality_stats path =
    match String_map.find !_fastx_quality_stats_cache path with
    | Some r -> return r
    | None ->
      read_file path >>| String.split ~on:'\n'
      >>| List.map ~f:(fun l -> String.split ~on:'\t' l)
      >>| List.filter ~f:(function [] | [_] -> false | _ -> true)
      >>= (function
      | [] | _ :: [] -> error (`empty_fastx_quality_stats path)
      | h :: t ->
        try List.map t ~f:(List.map ~f:Float.of_string) |! return with
          e -> error (`error_in_fastx_quality_stats_parsing (path, t)))
      >>| List.map ~f:(function
      | [bfxqs_column; bfxqs_count; bfxqs_min; bfxqs_max;
         bfxqs_sum; bfxqs_mean; bfxqs_Q1; bfxqs_med;
         bfxqs_Q3; bfxqs_IQR; bfxqs_lW; bfxqs_rW;
         bfxqs_A_Count; bfxqs_C_Count; bfxqs_G_Count; bfxqs_T_Count; bfxqs_N_Count;
         bfxqs_Max_count;] ->
        Some {
          bfxqs_column; bfxqs_count; bfxqs_min; bfxqs_max;
          bfxqs_sum; bfxqs_mean; bfxqs_Q1; bfxqs_med;
          bfxqs_Q3; bfxqs_IQR; bfxqs_lW; bfxqs_rW;
          bfxqs_A_Count; bfxqs_C_Count; bfxqs_G_Count; bfxqs_T_Count; bfxqs_N_Count;
          bfxqs_Max_count;}
      | _ -> None)
      >>| List.filter_opt
      >>= function
      | [] -> error (`error_in_fastx_quality_stats_parsing (path, []))
      | l ->
        _fastx_quality_stats_cache :=
          String_map.add ~key:path ~data:l !_fastx_quality_stats_cache;
        return l


end

let filter_map l ~filter ~map =
  List.filter_map l ~f:(fun x -> if filter x then Some (map x) else None)
let get_by_id l i = List.find l (fun o -> o#g_id = i)
let get_by_id_or_error l i =
  match List.find l (fun o -> o#g_id = i) with Some s -> return s
  | None -> error (`Layout (`Identification, `result_not_unique []))

let getopt_by_id l i =
  List.find l (fun o -> Some o#g_id = Option.map i (fun i -> i#id))

let rec volume_path ~configuration vols v =
  let open Layout.File_system in
  let open Option in
  Configuration.path_of_volume_fun configuration >>= fun path_fun ->
  begin match v#g_content with
  | Tree (hr_tag, trees) ->
    let vol = Common.volume_unix_directory ~id:v#g_id ~kind:v#g_kind ?hr_tag in
    return (path_fun vol)
  | Link p ->
    (get_by_id vols p.id
     >>= fun vv ->
     volume_path ~configuration vols vv)
  end

let rec all_paths_of_volume ~configuration volumes volume_id =
  let open Layout.File_system in
  get_by_id_or_error volumes volume_id >>= fun volume ->
  begin match volume#g_content with
  | Tree (hr_tag, trees) ->
    let relative_paths = Common.trees_to_unix_relative_paths trees in
    let vol =
      Common.volume_unix_directory ~id:(volume#g_id)
        ~kind:volume#g_kind ?hr_tag in
    begin match Configuration.path_of_volume_fun configuration with
    | Some vol_path ->
      return (List.map relative_paths (Filename.concat (vol_path vol)))
    | None ->
      error `root_directory_not_configured
    end
  | Link pointer ->
    all_paths_of_volume ~configuration volumes pointer.id
  end

let make_classy_libraries_information ~configuration ~layout_cache =
  let creation_started_on = Time.now () in
  layout_cache#hiseq_raw >>= fun hiseq_raws ->
  layout_cache#assemble_sample_sheet >>= fun ssassemblies ->
  layout_cache#sample_sheet >>= fun sample_sheets ->
  layout_cache#bcl_to_fastq >>= fun b2fs ->
  layout_cache#bcl_to_fastq_unaligned >>= fun b2fus ->
  layout_cache#file_system >>= fun volumes ->
  layout_cache#prepare_unaligned_delivery >>= fun udeliveries ->
  layout_cache#client_fastqs_dir >>= fun client_dirs ->
  layout_cache#generic_fastqs >>= fun generic_fastqs ->
  layout_cache#fastx_quality_stats >>= fun fxqss ->
  layout_cache#fastx_quality_stats_result >>= fun fxqs_rs ->
  layout_cache#person >>= fun persons ->
  layout_cache#protocol >>= fun protocols ->

  while_sequential b2fs (fun b2f ->
    let b2fu = getopt_by_id b2fus b2f#g_result in
    let hiseq_raw = get_by_id hiseq_raws b2f#raw_data#id in
    let vol =
      Option.(b2fu >>= fun b2fu ->
              hiseq_raw >>= fun hiseq_raw ->
              get_by_id volumes b2fu#directory#id
              >>= fun vol ->
              volume_path ~configuration volumes vol
              >>= fun path ->
              Some (vol , hiseq_raw, path, b2fu)) in
    map_option vol (fun (vol, hiseq_raw, path, b2fu) ->
      let basecall_stats_path =
        Filename.concat path
          (sprintf "Unaligned/Basecall_Stats_%s/%s"
             hiseq_raw#flowcell_name "Flowcell_demux_summary.xml") in
      let dmux_summary_m =
        File_cache.get_demux_summary basecall_stats_path in
      double_bind dmux_summary_m
        ~ok:(fun dmux_summary -> return (Some dmux_summary))
        ~error:(fun e ->
          (* eprintf "Error getting: %s\n%!" basecall_stats_path; *)
          return None)
      >>= fun dmux_summary ->
      let generic_vols =
        List.filter volumes (fun v ->
          v#g_content = Layout.File_system.Link vol#g_pointer)
      in
      let generics =
        List.map generic_vols (fun gv ->
          List.filter generic_fastqs (fun gf ->
            gf#directory#id = gv#g_id)) |! List.concat in
      let fastx_qss =
        List.map generics (fun g ->
          List.filter fxqss (fun f -> f#input_dir#id = g#g_id)) |! List.concat in
      let fastx_results =
        List.filter_map fastx_qss (fun fxqs ->
          getopt_by_id fxqs_rs fxqs#g_result) in
      let fastx_paths =
        List.filter_map fastx_results (fun fr ->
          let open Option in
          get_by_id volumes fr#directory#id
          >>= volume_path ~configuration volumes) in

      return (object
        method vol = vol
        method path = path
        method dmux_summary = dmux_summary
        method hiseq_raw = hiseq_raw
        method b2fu = b2fu
        method generic_vols = generic_vols
        method generics = generics
        method fastx_qss = fastx_qss
        method fastx_results = fastx_results
        method fastx_paths = fastx_paths
      end)))
  >>| List.filter_opt
  >>= fun unaligned_volumes ->

  layout_cache#flowcell >>= fun flowcells ->
  layout_cache#lane >>= fun lanes ->
  layout_cache#input_library >>= fun input_libraries ->

  while_sequential flowcells ~f:(fun fc ->
    while_sequential (Array.to_list fc#lanes) (fun lp ->
      get_by_id_or_error lanes lp#id >>= fun lane ->
      while_sequential (Array.to_list lane#contacts) (fun c ->
        List.find_exn persons ~f:(fun p -> p#g_id = c#id) |! return)
      >>= fun contacts ->
      while_sequential (Array.to_list lane#libraries)
        (fun l -> get_by_id_or_error input_libraries l#id)
      >>= fun libs ->
      return (object method oo = lane
                     method inputs = libs
                     method contacts = contacts end))
    >>= fun lanes ->
    let hiseq_raws =
      filter_map hiseq_raws
        ~filter:(fun h -> h#flowcell_name = fc#serial_name)
        ~map:(fun h ->
          let demuxes =
            filter_map b2fs ~filter:(fun b -> b#raw_data#id = h#g_id)
              ~map:(fun b2f ->
                let unaligned =
                  Option.(getopt_by_id b2fus b2f#g_result
                          >>= fun u ->
                          List.find unaligned_volumes (fun uv ->
                            uv#vol#g_id = u#directory#id)) in
                let sample_sheet = get_by_id sample_sheets b2f#sample_sheet#id in
                let assembly =
                  Option.(sample_sheet >>= fun s ->
                          List.find_map ssassemblies (fun a ->
                            a#g_result >>= fun r ->
                            if r#id = s#g_id then return a else None)) in
                let deliveries =
                  Option.map unaligned ~f:(fun unalig ->
                    filter_map udeliveries
                      ~filter:(fun u -> u#unaligned#id = unalig#b2fu#g_id)
                      ~map:(fun u ->
                        let dir = getopt_by_id client_dirs u#g_result in
                        (object method oo = u (* delivery *)
                                method client_fastqs_dir = dir end))) in
                (object
                  method b2f = b2f (* demultiplexing *)
                  method sample_sheet = sample_sheet
                  method assembly = assembly
                  method unaligned = unaligned
                  method deliveries = Option.value ~default:[] deliveries
                 end)) in
          (object (* hiseq_raw *)
            method oo = h method demultiplexings = demuxes end))
    in
    return (object method oo = fc (* Flowcell *)
                   method lanes = lanes
                   method hiseq_raws = hiseq_raws end))
  >>= fun flowcells ->

  layout_cache#bioanalyzer
  >>= while_sequential ~f:(fun ba ->
    map_option Option.(ba#files >>= fun v -> get_by_id volumes v#id) (fun vol ->
      all_paths_of_volume ~configuration volumes vol#g_id)
    >>= fun paths ->
    return (object
      method bioanalyzer = ba
      method paths = Option.value ~default:[] paths
    end))
  >>= fun bioanalyzers ->

  layout_cache#agarose_gel
  >>= while_sequential ~f:(fun ag ->
    map_option Option.(ag#files >>= fun v -> get_by_id volumes v#id) (fun vol ->
      all_paths_of_volume ~configuration volumes vol#g_id)
    >>= fun paths ->
    return (object
      method agarose_gel = ag
      method paths = Option.value ~default:[] paths
    end))
  >>= fun agarose_gels ->

  layout_cache#sample >>= fun samples ->
  layout_cache#organism >>= fun organisms ->
  layout_cache#barcode >>= fun barcodes ->
  layout_cache#invoicing >>= fun invoices ->
  layout_cache#stock_library
  >>= while_sequential ~f:(fun sl ->
    let optid =  Option.map ~f:(fun o -> o#id) in
    let sample = List.find_map samples (fun s ->
        if Some s#g_id = optid sl#sample then (
          let org = List.find organisms (fun o -> Some o#g_id = optid s#organism) in
          Some (object method sample = s method organism = org end))
        else None) in
    let (submissions: _ submission list) =
      List.map flowcells (fun fc ->
        List.filter_mapi fc#lanes ~f:(fun idx l ->
          if List.exists l#inputs (fun i -> i#library#id = sl#g_id)
          then Some (idx, l)
          else None)
        |! List.map ~f:(fun (idx, l) ->
          let invoices =
            List.filter invoices (fun i ->
              Array.exists i#lanes (fun ll -> ll#id = l#oo#g_id)) in
          (object method flowcell = fc (* submission *)
                  method lane_index = idx + 1
                  method lane = l
                  method invoices = invoices
           end))) |! List.concat in
    let preparator =
      List.find persons (fun p ->
        Some p#g_pointer = Option.map sl#preparator (fun p -> p#pointer)) in
    let barcoding =
      List.map (Array.to_list sl#barcoding)
        (fun il -> List.filter_map (Array.to_list il) ~f:(fun i ->
          List.find barcodes (fun b -> b#g_id = i#id))) in
    let bios =
      List.filter bioanalyzers (fun b -> b#bioanalyzer#library#id = sl#g_id) in
    let agels =
      List.filter agarose_gels (fun b -> b#agarose_gel#library#id = sl#g_id) in
    let prot =
      List.find protocols (fun p ->
        Some p#g_pointer = Option.map sl#protocol (fun x -> x#pointer)) in
    map_option Option.(prot >>= fun v -> get_by_id volumes v#doc#id) (fun vol ->
      all_paths_of_volume ~configuration volumes vol#g_id)
    >>= fun protocol_paths ->
    return (object
      method stock = sl (* library *)
      method submissions = submissions
      method sample = sample
      method barcoding = barcoding
      method preparator = preparator
      method bioanalyzers = bios
      method agarose_gels = agels
      method protocol = prot
      method protocol_paths = protocol_paths
    end: _ classy_library))
  >>= fun libraries ->
  let created = Time.now () in
  return (object (self)
    method creation_started_on = creation_started_on
    method created_on = created
    method configuration = configuration
    method libraries = libraries
  end: _ classy_libraries_information)

let qualified_name po n =
  sprintf "%s%s" Option.(value_map ~default:"" ~f:(sprintf "%s.") po) n

let get_fastq_stats lib sub dmux =
  let open Option in
  dmux#unaligned
  >>= fun u ->
  u#dmux_summary
  >>= fun ds ->
  let module Bui = Hitscore_interfaces.B2F_unaligned_information in
  List.find ds.(sub#lane_index - 1) (fun x ->
    x.Bui.name = lib#stock#name)

let choose_delivery_for_user dmux sub =
  let open Option in
  List.filter_map dmux#deliveries (fun del ->
    if del#oo#g_status <> `Succeeded
    then None
    else (
      List.find sub#invoices (fun i -> i#g_id = del#oo#invoice#id)
                                       >>= fun inv ->
      Some (del, inv)))
  |! List.reduce ~f:(fun (d1, i1) (d2, i2) ->
    if d1#oo#g_completed < d2#oo#g_completed
    then (d2, i2)
    else (d1, i1))


let fastq_path unaligned_path lane_index lib_name barcode read =
  sprintf "%s/Unaligned/Project_Lane%d/Sample_%s/%s_%s_L00%d_R%d_001.fastq.gz"
    unaligned_path lane_index lib_name lib_name barcode
    lane_index read

let fastxqs_path fastxqs_path lane_index lib_name barcode read =
  sprintf "%s/Unaligned/Project_Lane%d/Sample_%s/%s_%s_L00%d_R%d_001.fxqs"
    fastxqs_path lane_index lib_name lib_name barcode
    lane_index read

let demuxable_barcode_sequences lane lib =
  let module Barcodes = Hitscore_assemble_sample_sheet.Assemble_sample_sheet in
  if List.length lane#inputs = 1
  then ["NoIndex"; "Undetermined"]
  else
    begin match lib#barcoding with
    | [and_list] ->
      if List.for_all and_list (fun o ->
        (o#kind = `illumina || o#kind = `bioo) && o#index <> None)
      then
        List.filter_map and_list (fun o ->
          match o#kind with
          | `illumina ->
            List.Assoc.find
              Barcodes.illumina_barcodes (Option.value_exn o#index)
          | `bioo ->
            List.Assoc.find Barcodes.bioo_barcodes (Option.value_exn o#index)
          | _ -> None)
      else []
    | _ -> []
    end

let user_file_paths ~unaligned ~submission lib =
  let barcodes = demuxable_barcode_sequences submission#lane lib in
  let fastq_r1s =
    List.map barcodes (fun seq ->
      fastq_path unaligned#path submission#lane_index lib#stock#name seq 1) in
  let fastq_r2s =
    Option.value_map ~default:[]
      submission#lane#oo#requested_read_length_2 ~f:(fun _ ->
        List.map barcodes (fun seq ->
          fastq_path unaligned#path submission#lane_index lib#stock#name seq 2))
  in
  let fastx =
    List.map unaligned#fastx_paths (fun path ->
      let fastxqs_r1s =
        List.map barcodes (fun seq ->
          (object
            method kind = "R1"
            method path =
              fastxqs_path path submission#lane_index lib#stock#name seq 1
           end))
      in
      let fastxqs_r2s =
        Option.value_map ~default:[]
          submission#lane#oo#requested_read_length_2 ~f:(fun _ ->
            List.map barcodes (fun seq ->
              (object
                method kind = "R2"
                method path =
                  fastxqs_path path submission#lane_index lib#stock#name seq 2
               end)))
      in
      (fastxqs_r1s, fastxqs_r2s)) in
  (object
    method fastq_r1s = fastq_r1s
    method fastq_r2s = fastq_r2s
    method fastx = fastx
   end)


let db_connect ?log t =
  let open Configuration in
  let host, port, database, user, password =
    db_host t, db_port t, db_database t, db_username t, db_password t in
  Backend.connect ?host ?port ?database ?user ?password ?log ()

let init_some_retrieval_loop ~log ~log_prefix ~loop_waiting_time
    ~allowed_age ~maximal_age ~configuration ~f =
  let info_mem = ref None in
  let logf fmt = ksprintf log fmt in
  let condition = Lwt_condition.create () in
  let should_reconnect = ref false in
  let rec update ~configuration ~layout_cache =
    let m =
      let starting_time = Time.now () in
      if !should_reconnect
      then begin
        Backend.reconnect layout_cache#dbh
        >>= fun _ ->
        logf "%s: successfully reconnected." log_prefix
        >>= fun () ->
        should_reconnect := false;
        return ()
      end
      else return ()
      >>= fun _ ->
      f ~configuration ~layout_cache
      >>= fun info ->
      info_mem := Some info;
      Lwt_condition.broadcast condition info;
      return ()
      >>= fun () ->
      logf  "%s updated:\n\
               \          %s --> %s\n\
               \          %g secs\n%!"
        log_prefix
        Time.(starting_time |! to_string)
        Time.(now () |! to_string)
        Time.(to_float (now ()) -. to_float starting_time);
    in
    double_bind m
      ~ok:return
      ~error:(fun e ->
        wrap_io (Lwt_io.eprintf "%S Updating gave an error; reconnecting …\n")
          log_prefix
        >>= fun () ->
        should_reconnect := true;
        logf "%s info gave an error:\n%s" log_prefix
          (match e with
          | `Layout (loc, cause) ->
            sprintf "LAYOUT-ERROR: %s: %s"
              (Layout.sexp_of_error_location loc |! Sexp.to_string)
              (Layout.sexp_of_error_cause cause |! Sexp.to_string)
          | `db_backend_error er ->
            sprintf "BACKEND-ERROR: %s"
              (Backend.sexp_of_error er |! Sexp.to_string)
          | `io_exn ex ->
            sprintf "EXCEPTION: %s" Exn.(to_string ex)
          | `root_directory_not_configured ->
            "root_directory_not_configured"
          | _ -> "Unknown")
        >>= fun () ->
        return ())
    >>= fun () ->
    wrap_io Lwt_unix.sleep loop_waiting_time
    >>= fun () ->
    update ~configuration ~layout_cache in
  Lwt.ignore_result (
    db_connect configuration >>= fun dbh ->
    let layout_cache =
      Classy.make_cache ~allowed_age ~maximal_age ~dbh in
    update ~configuration ~layout_cache
  );
  begin fun () ->
    begin match !info_mem with
    | None ->
      wrap_io Lwt_condition.wait condition
      >>= fun info ->
      return info
    | Some info ->
      return info
    end
  end

let init_classy_libraries_information_loop =
  init_some_retrieval_loop ~f:(make_classy_libraries_information)
    ~log_prefix:"libraries-classy-info"

let make_classy_persons_information
    ~configuration  ~(layout_cache: _ Classy.layout_cache) =
  let creation_started_on = Time.now () in
  layout_cache#person >>= fun persons ->
  layout_cache#affiliation >>= fun affiliations ->
  layout_cache#authentication_token >>= fun tokens ->
  layout_cache#lane >>= fun lanes ->
  layout_cache#input_library >>= fun input_libraries ->
  layout_cache#stock_library >>= fun stock_libraries ->
  while_sequential persons
    begin fun person ->
      let lanes_involved =
        List.filter lanes (fun l ->
          Array.exists l#contacts (fun p -> p#id = person#g_id)) in
      let libraries =
        List.map lanes_involved (fun l ->
          List.filter input_libraries (fun il ->
            Array.exists l#libraries (fun lib -> lib#id = il#g_id))
          |! List.filter_map ~f:(fun il ->
            List.find stock_libraries (fun sl -> sl#g_id = il#library#id)))
        |! List.concat
      in
      let tokens =
        List.filter tokens (fun t ->
          Array.exists person#auth_tokens (fun at -> at#id = t#g_id)) in
      return (object
        method lanes = lanes_involved
        method libraries = libraries
        method tokens = tokens
        method t = person
      end : _ person)
    end
  >>= fun persons ->
  let creation_finished_on = Time.now () in
  return (object
    method persons = persons
    method creation_start_time = creation_started_on
    method creation_end_time = creation_finished_on
  end: _ classy_persons_information)

let init_classy_persons_information_loop =
  init_some_retrieval_loop ~f:(make_classy_persons_information)
    ~log_prefix:"persons-classy-info"
