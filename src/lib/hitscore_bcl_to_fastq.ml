open Hitscore_std
open Hitscore_layout
open Hitscore_configuration
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Common

module Bcl_to_fastq: Hitscore_function_interfaces.BCL_TO_FASTQ = struct


  let pbs_fun =
    Common.PBS.make ~pbs_script_filename:"make_b2f.pbs" "b2f"

  let start ~dbh ~configuration
      ~(sample_sheet: Layout.Record_sample_sheet.pointer)
      ~(hiseq_dir: Layout.Record_hiseq_raw.pointer)
      ?tiles
      ?bases_mask
      ?(mismatch=`one)
      ?(version=`casava_182)
      ?user
      ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
      ?queue
      ?(hitscore_command="echo hitscore should: ")
      ?(make_command="make -j8")
      ?(basecalls_path= "Data/Intensities/BaseCalls/")
      name =

    check_hiseq_raw_availability ~dbh ~hiseq_raw:hiseq_dir
    >>= fun (availability, all_deleted) ->
    Layout.Record_sample_sheet.(
      Access.Sample_sheet.get ~dbh sample_sheet
      >>= fun sh ->
      let file = sh.g_value.file in
      Common.all_paths_of_volume ~dbh ~configuration file
      >>= function
      | [one] -> return one
      | [] -> error (`empty_sample_sheet_volume (file, sample_sheet))
      | more -> error (`more_than_one_file_in_sample_sheet_volume
                          (file, sample_sheet, more)))
    >>= fun sample_sheet_path ->
    Layout.Record_hiseq_raw.(
      Access.Hiseq_raw.get ~dbh hiseq_dir >>= fun hr ->
      let hiseq_dir_name = hr.g_value.hiseq_dir_name in
      Common.hiseq_raw_full_path ~configuration hiseq_dir_name
      >>= fun hs_path ->
      return (Filename.concat hs_path basecalls_path))
    >>= fun basecalls ->
    let mismatch32 =
      match mismatch with `zero -> 0 | `one -> 1 | `two -> 2 in
    let casava_version =
      match version with `casava_182 -> "1.8.2" | `casava_181 -> "1.8.1" in
    let pre_commands =
      Configuration.bcl_to_fastq_pre_commands configuration casava_version in

    Layout.Function_bcl_to_fastq.(
      Access.Bcl_to_fastq.add_evaluation ~dbh
        ~raw_data:hiseq_dir ?tiles ?bases_mask ?basecalls_path
        ~availability ~mismatch:mismatch32 ~version:casava_version ~sample_sheet
      >>= fun b2f ->
      let log =
        sprintf "(create_bcl_to_fastq %d (raw_data %d) \
                        (availability %d) (mismatch %d) (version %s) \
                        %s %s (sample_sheet %d))"
          b2f.id
          hiseq_dir.Layout.Record_hiseq_raw.id
          availability.Layout.Record_inaccessible_hiseq_raw.id
          mismatch32 casava_version
          (Option.value_map ~default:"()" ~f:(sprintf "(tiles %S)") tiles)
          (Option.value_map ~default:"()"
             ~f:(sprintf "(bases_mask %S)") bases_mask)
          sample_sheet.Layout.Record_sample_sheet.id in
      Access.Log.add_value ~dbh ~log >>= fun _ ->
      Access.Bcl_to_fastq.set_started ~dbh b2f >>= fun () ->
      Access.Log.add_value ~dbh
        ~log:(sprintf "(set_bcl_to_fastq_started %d)" b2f.id) >>= fun _ ->
      return b2f
    )
    >>= fun started ->

    let after_start_m =
      let id =  started.Layout.Function_bcl_to_fastq.id in
      let pbs = pbs_fun id in
      let job_name = sprintf "HS-B2F-%d-%s" id name in
      Common.PBS.pbs_result_path ~configuration pbs >>| sprintf "%s/Unaligned"
      >>= fun unaligned ->
      Common.PBS.pbs_output_path ~configuration pbs
      >>= fun out_path ->
      let make_stdout_path = sprintf "%s/make.stdout" out_path in
      let make_stderr_path = sprintf "%s/make.stderr" out_path in
      Common.PBS.pbs_script ~configuration pbs
        ~nodes ~ppn ~wall_hours ?queue ?user ~job_name
        ~on_command_failure:(fun cmd ->
          sprintf "%s register-failure %d 'shell_command_failed %S'"
            hitscore_command id cmd)
        ~add_commands:(fun ~checked ~non_checked ->
          List.iter pre_commands (ksprintf checked "%s");
          ksprintf checked "cd %s" unaligned;
          ksprintf checked "%s 1> %s 2> %s"
            make_command make_stdout_path make_stderr_path;
          ksprintf checked
            "test `cat %s/Basecall_Stats_*/Demultiplex_Stats.htm | wc -l` -gt 5"
            unaligned;
          ksprintf checked
            "%s register-success %d" hitscore_command id;
          non_checked "echo \"Done: $(date -R)\"")
      >>= fun pbs_script ->
      let cmd fmt = ksprintf (fun s -> system_command s) fmt in
      Common.PBS.prepare_work_environment ~dbh ~configuration pbs
      >>= fun () ->
      let command_chain =
        (String.concat  pre_commands ~sep:"  &&  ")
        ^ (sprintf " && configureBclToFastq.pl --fastq-cluster-count 800000000 \
                    %s %s --input-dir %s \
                    --output-dir %s \
                    --sample-sheet %s \
                    --mismatches %d"
            (Option.value_map ~default:"" ~f:(sprintf "--tiles %S") tiles)
            (Option.value_map ~default:""
               ~f:(sprintf "--use-bases-mask %S") bases_mask)
          basecalls unaligned sample_sheet_path mismatch32) in
      cmd "%s" command_chain >>= fun () ->
      Common.PBS.qsub_pbs_script ~dbh ~configuration pbs pbs_script
    in
    double_bind after_start_m
      ~ok:(fun () -> return (`success started))
      ~error:(fun e ->
        Access.Bcl_to_fastq.set_failed ~dbh started
        >>= fun () ->
        Access.Log.add_value ~dbh
          ~log:(sprintf "(set_bcl_to_fastq_failed %d while_creating_starting)"
                  started.Layout.Function_bcl_to_fastq.id)
        >>= fun _ ->
        return (`failure (started, e)))

  let succeed ~dbh ~configuration ~bcl_to_fastq =
    let module LFB2F = Layout.Function_bcl_to_fastq in
    LFB2F.(Access.Bcl_to_fastq.get ~dbh bcl_to_fastq
           >>= fun {g_id; g_evaluation = {raw_data; sample_sheet}} ->
           return (sprintf "B2F%d_Raw%d_Sash%d" g_id
                     raw_data.Layout.Record_hiseq_raw.id
                     sample_sheet.Layout.Record_sample_sheet.id))
    >>= fun hr_tag ->
    Layout.File_system.(
      let files = Tree.([opaque "Unaligned"]) in
      Access.Volume.add_tree_volume
        ~dbh ~hr_tag ~kind:`bcl_to_fastq_unaligned_opaque ~files
      >>= fun vol ->
      Common.path_of_volume ~dbh ~configuration vol >>= fun vol_path ->
      return (vol, vol_path))
    >>= fun (directory, path_vol) ->
    let pbs = pbs_fun bcl_to_fastq.LFB2F.id in
    Common.PBS.pbs_result_path pbs ~configuration >>= fun result_root ->
    Common.PBS.save_pbs_runtime_information pbs ~dbh ~configuration path_vol
    >>= fun () ->
    let move_m =
      ksprintf system_command "mkdir -p %s/" path_vol >>= fun () ->
      ksprintf system_command "mv %s/Unaligned %s/Unaligned" result_root path_vol
      >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir path_vol) ~configuration
    in
    double_bind move_m
      ~ok:(fun () ->
        Access.Bcl_to_fastq_unaligned.add_value ~dbh ~directory
        >>= fun result ->
        Access.Bcl_to_fastq.set_succeeded ~dbh ~result bcl_to_fastq
        >>= fun () ->
        Access.Log.add_value ~dbh
          ~log:(sprintf "(set_bcl_to_fastq_succeeded %d (result %d))"
                  bcl_to_fastq.Layout.Function_bcl_to_fastq.id
                  result.Layout.Record_bcl_to_fastq_unaligned.id)
        >>= fun  _ ->
        return (`success bcl_to_fastq))
      ~error:(fun e ->
        Layout.File_system.(
          Access.Volume.(get ~dbh directory >>= fun vol ->
                         delete_volume_unsafe ~dbh vol)
          >>= fun () ->
          return (directory.id))
        >>= fun volid ->
        Access.Bcl_to_fastq.set_failed ~dbh bcl_to_fastq
        >>= fun () ->
        Access.Log.add_value ~dbh
          ~log:(sprintf "(delete_orphan_volume (id %d))" volid)
        >>= fun _ ->
        Access.Log.add_value ~dbh
          ~log:(sprintf "(set_bcl_to_fastq_failed %d)"
                  bcl_to_fastq.Layout.Function_bcl_to_fastq.id)
        >>= fun _ ->
        return (`failure (bcl_to_fastq, e)))


  include Common.Make_pbs_function (struct
    include Layout.Function_bcl_to_fastq
    let set_failed = Access.Bcl_to_fastq.set_failed
    let get_status p ~dbh =
      Access.Bcl_to_fastq.get ~dbh p
      >>= fun b2f ->
      return Layout.Function_bcl_to_fastq.(b2f.g_status)
    let pbs_fun = pbs_fun
    let name_in_log = "bcl_to_fastq"
  end)



end
