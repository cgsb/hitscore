open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend

open Hitscore_common
open Common

module Unaligned_delivery:
  Hitscore_function_interfaces.UNALIGNED_DELIVERY
= struct

  let debug fmt =
    ksprintf debug fmt
  let cmd fmt =
    ksprintf system_command fmt

  let sanitize_filename s = String.map s ~f:(function
    | 'a' .. 'z' | 'A' .. 'Z' | '-' | '_' | '0' .. '9' as c -> c
    | _ -> '_')

  let run ~dbh ~configuration
      ?directory_tag ~bcl_to_fastq ~invoice ~destination =
    let layout = Classy.make dbh in
    layout#bcl_to_fastq#get bcl_to_fastq
    >>= fun b2f ->
    begin match b2f#g_status, b2f#g_result with
    | `Succeeded, Some result -> return result
    | status, _ -> error (`bcl_to_fastq_not_succeeded (bcl_to_fastq ,status))
    end
    >>= fun unaligned ->
    layout#bcl_to_fastq_unaligned#get unaligned#pointer
    >>= fun b2fu ->
    return b2fu#directory#pointer
    >>= Common.all_paths_of_volume ~dbh ~configuration
    >>= (function [one] -> return one | l -> error (`wrong_unaligned_volume l))
    >>= fun unaligned_path ->
    debug "unaligned_path: %S\n" unaligned_path >>= fun () ->
    layout#invoicing#get invoice
    >>= fun invoice ->
    invoice#pi#get
    >>= fun pi ->
    return (pi#family_name, invoice#lanes)
    >>= fun (pi_name, invoice_lanes) ->
    debug "pi: %s; lanes: %s\n" pi_name 
      (Array.to_list invoice_lanes 
       |! List.map ~f:(fun l -> sprintf "%d" l#id) |! String.concat ~sep:", ")
    >>= fun () ->
    layout#flowcell#all >>= fun flowcells ->
    of_list_sequential flowcells ~f:(fun fc ->
      let find_lanes =
        Array.map invoice_lanes
          ~f:(fun l -> Array.findi ~f:(fun _ o -> o#pointer = l#pointer) fc#lanes) 
        |! Array.to_list in
      if List.for_all find_lanes ~f:((=) None) then
        return None
      else if List.exists find_lanes ((=) None) then
        error (`partially_found_lanes (fc#g_id, fc#serial_name))
      else
        return (Some (fc#serial_name,
                      List.filter_opt find_lanes |! List.map ~f:fst)))
    >>| List.filter_opt >>= function
    | [ (flowcell_name, lanes_idxs) ]->
      debug "%s wants lanes: %s of %s\n" pi_name 
        (lanes_idxs |! List.map ~f:(sprintf "%d") |! String.concat ~sep:", ")
        flowcell_name >>= fun () ->
      of_list_sequential (Array.to_list invoice_lanes) ~f:(fun il ->
        il#get >>= fun invlane ->
        of_list_sequential (Array.to_list invlane#contacts) ~f:(fun cp ->
          cp#get >>| fun c -> c#login)
        >>| List.filter_opt)
      >>| List.concat >>| List.dedup
      >>= fun logins ->
      debug "logins: %s\n" (String.concat ~sep:", " logins) >>= fun () ->
      let links_dir =
        let default = Time.(now () |! to_local_date) |! Date.to_string in
        sprintf "%s/%s/%s_%s"
          destination (sanitize_filename pi_name) 
          (Option.value ~default directory_tag) flowcell_name
      in          
      cmd "mkdir -m 750 -p %S" links_dir  >>= fun () ->
      debug "created links dir: %s\n" links_dir >>= fun () ->
      of_list_sequential lanes_idxs ~f:(fun idx ->
        cmd "unset CDPATH; cd %s && ln -s %s/Project_Lane%d Lane%d"
          links_dir unaligned_path (idx + 1) (idx + 1))
      >>= fun (_: unit list) ->
      Access_rights.set_posix_acls ~dbh 
        ~more_readers:logins ~configuration (`dir links_dir)
      >>= fun () ->
      layout#add_prepare_unaligned_delivery
        ~recomputable:false ~recompute_penalty:0.
        ~unaligned:unaligned#pointer ~invoice:invoice#g_pointer ()
      >>= fun fpointer ->
      fpointer#get >>= fun f ->
      f#set_started >>= fun () ->
      layout#add_client_fastqs_dir ~directory:links_dir ()
      >>= fun result ->
      f#set_succeeded result#pointer
      >>= fun () ->
      return fpointer#pointer 
    | l -> 
      error (`not_single_flowcell l)


end
