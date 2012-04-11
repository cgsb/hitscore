
module Make
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.BCL_TO_FASTQ
  with module Common = Common
 = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

    let pbs_fun =
      Common.PBS.make ~pbs_script_filename:"make_b2f.pbs" "b2f"
      
    let start ~dbh ~configuration
        ~(sample_sheet: Layout.Record_sample_sheet.pointer)
        ~(hiseq_dir: Layout.Record_hiseq_raw.pointer)
        ?tiles
        ?bases_mask
        ?(mismatch=`one)
        ?(version=`casava_182)
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(queue="cgsb-s")
        ?(hitscore_command="echo hitscore should: ")
        ?(make_command="make -j8")
        name =

      check_hiseq_raw_availability ~dbh ~hiseq_raw:hiseq_dir
      >>= fun (availability, all_deleted) ->
      Layout.Record_sample_sheet.(
        get ~dbh sample_sheet
        >>= fun { file; _ } ->
        Common.all_paths_of_volume ~dbh ~configuration file
        >>= function
        | [one] -> return one
        | [] -> error (`empty_sample_sheet_volume (file, sample_sheet))
        | more -> error (`more_than_one_file_in_sample_sheet_volume 
                            (file, sample_sheet, more)))
      >>= fun sample_sheet_path ->
      Layout.Record_hiseq_raw.(
        get ~dbh hiseq_dir >>= fun {hiseq_dir_name; _} ->
        Common.hiseq_raw_full_path ~configuration hiseq_dir_name
        >>= fun hs_path ->
        return (Filename.concat hs_path "Data/Intensities/BaseCalls/"))
      >>= fun basecalls ->
      let mismatch32 =
        match mismatch with `zero -> 0l | `one -> 1l | `two -> 2l in
      let casava_version =
        match version with `casava_182 -> "1.8.2" | `casava_181 -> "1.8.1" in

      Layout.Function_bcl_to_fastq.(
          add_evaluation ~dbh
            ~raw_data:hiseq_dir ?tiles ?bases_mask
            ~availability ~mismatch:mismatch32 ~version:casava_version
            ~sample_sheet ~recomputable:true ~recompute_penalty:100.
          >>= fun b2f ->
          let log =
            sprintf "(create_bcl_to_fastq %ld (raw_data %ld) \
                        (availability %ld) (mismatch %ld) (version %s) \
                        %s %s (sample_sheet %ld))"
              b2f.id
              hiseq_dir.Layout.Record_hiseq_raw.id
              availability.Layout.Record_inaccessible_hiseq_raw.id
              mismatch32 casava_version
              (Option.value_map ~default:"()" ~f:(sprintf "(tiles %S)") tiles)
              (Option.value_map ~default:"()"
                 ~f:(sprintf "(bases_mask %S)") bases_mask)
              sample_sheet.Layout.Record_sample_sheet.id in
          Layout.Record_log.add_value ~dbh ~log >>= fun _ ->
          set_started ~dbh b2f >>= fun b2f ->
          Layout.Record_log.add_value ~dbh 
            ~log:(sprintf "(set_bcl_to_fastq_started %ld)" b2f.id) >>= fun _ ->
          return b2f
        )
      >>= fun started ->

      let after_start_m =
        let id =  started.Layout.Function_bcl_to_fastq.id in
        let pbs = pbs_fun id in
        let job_name = sprintf "HS-B2F-%ld-%s" id name in
        Common.PBS.pbs_result_path ~configuration pbs >>| sprintf "%s/Unaligned"
        >>= fun unaligned ->
        Common.PBS.pbs_output_path ~configuration pbs
        >>= fun out_path ->
        let make_stdout_path = sprintf "%s/make.stdout" out_path in
        let make_stderr_path = sprintf "%s/make.stderr" out_path in
        Common.PBS.pbs_script ~configuration pbs
          ~nodes ~ppn ~wall_hours ~queue ~user ~job_name
          ~on_command_failure:(fun cmd ->
            sprintf "%s register-failure %ld 'shell_command_failed %S'"
              hitscore_command id cmd)
          ~add_commands:(fun ~checked ~non_checked ->
            ksprintf checked ". /share/apps/casava/%s/intel/env.sh" casava_version;
            ksprintf checked "cd %s" unaligned;
            ksprintf checked "%s 1> %s 2> %s" 
              make_command make_stdout_path make_stderr_path;
            ksprintf checked
              "test `cat %s/Basecall_Stats_*/Demultiplex_Stats.htm | wc -l` -gt 5"
              unaligned;
            ksprintf checked 
              "%s register-success %ld" hitscore_command id;
            non_checked "echo \"Done: $(date -R)\"")
        >>= fun pbs_script ->
        let cmd fmt = ksprintf (fun s -> system_command s) fmt in 
        Common.PBS.prepare_work_environment ~dbh ~configuration pbs
        >>= fun () ->
        cmd ". /share/apps/casava/%s/intel/env.sh && \
                  configureBclToFastq.pl --fastq-cluster-count 800000000 \
                    %s %s --input-dir %s \
                    --output-dir %s \
                    --sample-sheet %s \
                    --mismatches %ld"
          casava_version 
          (Option.value_map ~default:"" ~f:(sprintf "--tiles %S") tiles)
          (Option.value_map ~default:""
             ~f:(sprintf "--use-bases-mask %S") bases_mask)
          basecalls unaligned sample_sheet_path mismatch32
        >>= fun () ->
        Common.PBS.qsub_pbs_script ~dbh ~configuration pbs pbs_script
      in
      double_bind after_start_m
        ~ok:(fun () -> return (`success started))
        ~error:(fun e ->
          Layout.Function_bcl_to_fastq.set_failed ~dbh started
          >>= fun failed ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_bcl_to_fastq_failed %ld while_creating_starting)" 
                    failed.Layout.Function_bcl_to_fastq.id)
          >>= fun _ ->
          return (`failure (failed, e)))
        
    let succeed ~dbh ~configuration ~bcl_to_fastq =
      let module LFB2F = Layout.Function_bcl_to_fastq in
      LFB2F.(get ~dbh bcl_to_fastq >>= fun {g_id; raw_data; sample_sheet} ->
             return (sprintf "B2F%ld_Raw%ld_Sash%ld" g_id
                       raw_data.Layout.Record_hiseq_raw.id
                       sample_sheet.Layout.Record_sample_sheet.id))
      >>= fun hr_tag ->
      Layout.File_system.(
        let files = Tree.([opaque "Unaligned"]) in
        add_volume ~dbh ~hr_tag ~kind:`bcl_to_fastq_unaligned_opaque ~files
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
          Layout.Record_bcl_to_fastq_unaligned.add_value ~dbh ~directory
          >>= fun result ->
          Layout.Function_bcl_to_fastq.set_succeeded ~dbh ~result bcl_to_fastq
          >>= fun success ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_bcl_to_fastq_succeeded %ld (result %ld))" 
                    success.Layout.Function_bcl_to_fastq.id
                    result.Layout.Record_bcl_to_fastq_unaligned.id)
          >>= fun  _ ->
          return (`success success))
        ~error:(fun e ->
          Layout.File_system.(
            delete_volume ~dbh directory.id
            >>= fun () ->
            return (directory.id))
          >>= fun volid ->
          Layout.Function_bcl_to_fastq.set_failed ~dbh bcl_to_fastq
          >>= fun failed ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(delete_orphan_volume (id %ld))" volid)
          >>= fun _ ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_bcl_to_fastq_failed %ld)" 
                    failed.Layout.Function_bcl_to_fastq.id)
          >>= fun _ ->
          return (`failure (failed, e)))


    include Common.Make_pbs_function (Layout.Function_bcl_to_fastq) (struct
      let pbs_fun = pbs_fun
      let name_in_log = "bcl_to_fastq"
    end)



  end
