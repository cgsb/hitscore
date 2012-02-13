module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t)
  (Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
     with module Result_IO = Result_IO
     with module Configuration = Configuration
     with module Layout = Layout):

  Hitscore_function_interfaces.UNALIGNED_DELIVERY
  with module Configuration = Configuration
  with module Result_IO = Result_IO
  with module Access_rights = Access_rights
  with module Layout = Layout

  = struct

    module Configuration = Configuration
    module Result_IO = Result_IO
    module Access_rights = Access_rights
    module Layout = Layout

    open Hitscore_std
    open Result_IO

    let volume_relative_paths ~dbh ~configuration vol_pointer =
      Layout.File_system.(
        get_volume ~dbh vol_pointer >>= fun ({volume_entry; _} as volume) ->
        of_result  (volume_trees volume) >>= fun vol_trees ->
        match (Configuration.work_directory configuration) with
        | None -> error (`work_directory_not_configured)
        | Some w ->
          let prefix = w ^ entry_unix_path volume_entry in
          return (List.map (trees_to_unix_paths vol_trees) 
                    ~f:(Filename.concat prefix)))
 
    let debug fmt =
      ksprintf debug fmt
    let cmd fmt =
      ksprintf system_command fmt

    let sanitize_filename s = String.map s ~f:(function
      | 'a' .. 'z' | 'A' .. 'Z' | '-' | '_' | '0' .. '9' as c -> c
      | _ -> '_')

    let run ~(dbh:Layout.db_handle) ~configuration
        ~bcl_to_fastq ~invoice ~destination =
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq >>= function
        | {g_status = `Succeeded; g_result = Some result; _} ->
          return result
        | {g_status} ->
          error (`bcl_to_fastq_not_succeeded (bcl_to_fastq ,g_status)))
      >>= fun unaligned ->
      Layout.Record_bcl_to_fastq_unaligned.(
        get ~dbh unaligned >>= fun {directory} -> return directory)
      >>= volume_relative_paths ~dbh ~configuration 
      >>= (function [one] -> return one | l -> error (`wrong_unaligned_volume l))
      >>= fun unaligned_path ->
      debug "unaligned_path: %S\n" unaligned_path >>= fun () ->
      Layout.Record_invoicing.(
        get ~dbh invoice >>= fun {pi; lanes; _} ->
        Layout.Record_person.(
          get ~dbh pi >>= fun {family_name} ->
          return (family_name, lanes)))
      >>= fun (pi_name, invoice_lanes) ->
      debug "pi: %s; lanes: %s\n" pi_name 
        (Array.to_list invoice_lanes 
         |! List.map ~f:(fun l -> sprintf "%ld" l.Layout.Record_lane.id)
         |! String.concat ~sep:", ") >>= fun () ->
      Layout.Record_flowcell.(
        get_all ~dbh >>= fun flowcells ->
        of_list_sequential flowcells ~f:(fun p ->
          get ~dbh p >>= fun {g_id; serial_name; lanes; } ->
          let find_lanes =
            Array.map invoice_lanes ~f:(fun l -> Array.findi ~f:((=) l) lanes) 
            |! Array.to_list in
          if List.for_all find_lanes ~f:((=) None) then
            return None
          else if List.exists find_lanes ((=) None) then
            error (`partially_found_lanes (g_id, serial_name,
                                           invoice_lanes, find_lanes))
          else
            return (Some (serial_name, List.filter_opt find_lanes))))
      >>| List.filter_opt >>= function
      | [ (flowcell_name, lanes_idxs) ]->
        debug "%s wants lanes: %s of %s\n" pi_name 
          (lanes_idxs |! List.map ~f:(sprintf "%d") |! String.concat ~sep:", ")
          flowcell_name >>= fun () ->
        of_list_sequential (Array.to_list invoice_lanes) ~f:(fun il ->
          Layout.Record_lane.(
            get ~dbh il >>= fun {contacts} ->
            of_list_sequential (Array.to_list contacts) ~f:(fun cp ->
              Layout.Record_person.(get ~dbh cp >>| fun c -> c.login)))
          >>| List.filter_opt)
        >>| List.flatten >>| List.dedup
        >>= fun logins ->
        debug "logins: %s\n" (String.concat ~sep:", " logins) >>= fun () ->
        let delivery_date = Time.(now () |! to_filename_string) in
        let links_dir = sprintf "%s/%s/%s_%s"
          destination (sanitize_filename pi_name) delivery_date flowcell_name
        in          
        cmd "mkdir -p %s" links_dir  >>= fun () ->
        debug "created links dir: %s\n" links_dir >>= fun () ->
        of_list_sequential lanes_idxs ~f:(fun idx ->
          cmd "unset CDPATH; cd %s && ln -s %s/Project_Lane%d Lane%d"
            links_dir unaligned_path (idx + 1) (idx + 1))
        >>= fun (_: unit list) ->
        Access_rights.set_posix_acls ~dbh 
          ~more_readers:logins ~configuration (`dir destination)
        >>= fun () ->
        Layout.Function_prepare_unaligned_delivery.(
          add_evaluation ~dbh ~recomputable:false ~recompute_penalty:0.
            ~unaligned ~invoice >>= fun fpointer ->
          set_started ~dbh fpointer >>= fun fpointer ->
          Layout.Record_client_fastqs_dir.(add_value ~dbh ~dir:links_dir)
          >>= fun result ->
          set_succeeded ~dbh ~result fpointer)
      | l -> 
        error (`not_single_flowcell l)


end
