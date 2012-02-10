
module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t)
  (ACL : Hitscore_acl.ACL 
     with module Result_IO = Result_IO
     with module Configuration = Configuration
     with module Layout = Layout)
  = struct

    module Configuration = Configuration
    module Result_IO = Result_IO
    module ACL = ACL
    module Layout = Layout

    open Hitscore_std
    open Result_IO

    let work_root_directory work_dir unique_id =
      sprintf "%s/B2F/work/%s/" work_dir unique_id

    let work_run_time work_dir db_id =
      sprintf "%s/B2F/run/%ld" work_dir db_id

    let start ~dbh ~configuration
        ~(sample_sheet: Layout.Record_sample_sheet.pointer)
        ~(hiseq_dir: Layout.Record_hiseq_raw.pointer)
        ~(availability: Layout.Record_inaccessible_hiseq_raw.pointer)
        ?tiles
        ?(mismatch=`one)
        ?(version=`casava_182)
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(queue="cgsb-s")
        ?(hitscore_command="echo hitscore should: ")
        ?(make_command="make -j8")
        ~write_file
        name =

      Layout.Record_inaccessible_hiseq_raw.(
        get ~dbh availability >>= fun {deleted} ->
        if Array.exists deleted ((=) hiseq_dir) then
          error (`hiseq_dir_deleted (hiseq_dir, availability))
        else
          return ())
      >>= fun () ->
      Layout.Record_sample_sheet.(
        get ~dbh sample_sheet
        >>= fun { file; _ } ->
        Layout.File_system.(
          get_volume ~dbh file
          >>= fun vc -> (volume_trees vc |! IO.return) >>| trees_to_unix_paths 
          >>= function
          | [one] ->
            let vol = vc.volume_entry |! entry_unix_path in
            begin match Configuration.volume_path_fun configuration with
            | Some vol_path ->
              return (sprintf "%s/%s" (vol_path vol) one)
            | None -> 
              error `root_directory_not_configured
            end
          | [] -> error (`empty_sample_sheet_volume (file, sample_sheet))
          | more -> error (`more_than_one_file_in_sample_sheet_volume 
                              (file, sample_sheet, more))))
      >>= fun sample_sheet_path ->
      Layout.Record_hiseq_raw.(
        get ~dbh hiseq_dir >>= fun {hiseq_dir_name; _} ->
        return (hiseq_dir_name ^ "/Data/Intensities/BaseCalls/"))
      >>= fun basecalls ->
      let mismatch32 =
        match mismatch with `zero -> 0l | `one -> 1l | `two -> 2l in
      let casava_version =
        match version with `casava_182 -> "1.8.2" | `casava_181 -> "1.8.1" in
      let hr_tag =
        sprintf "B2F_%s_S%ld_H%ld_M%ld_%s_%s"
          name
          sample_sheet.Layout.Record_sample_sheet.id
          hiseq_dir.Layout.Record_hiseq_raw.id
          mismatch32 casava_version user in
      let unique_id = hr_tag (* TODO: uniquify for real? *) in
      begin match Configuration.work_directory configuration with
      | Some work_dir -> return work_dir
      | None -> error `work_directory_not_configured
      end
      >>= fun work_dir ->
      let work_root = work_root_directory work_dir unique_id in
      let out_dir = sprintf "%s/Output/" work_root in
      let unaligned = sprintf "%s/Unaligned" work_root in
      let pbs_script_file = sprintf "%s/script.pbs" work_root in
      let pbs_script b2f = 
        let stdout_path = sprintf "%s/pbs.stdout" out_dir in
        let stderr_path = sprintf "%s/pbs.stderr" out_dir in
        let make_stdout_path = sprintf "%s/make.stdout" out_dir in
        let make_stderr_path = sprintf "%s/make.stderr" out_dir in
        let run_dir = 
          work_run_time work_dir b2f.Layout.Function_bcl_to_fastq.id in
        let resource_list =
          sprintf "%swalltime=%d:00:00\n"
            (match nodes, ppn with
            | 0, 0 -> ""
            | n, m -> sprintf "nodes=%d:ppn=%d," n m)
            wall_hours in
        let job_name = sprintf "HS-B2F-%s" name in
        let checked_command ?if_ok s =
          sprintf "echo \"$(date -R)\"\necho %S\n%s\nif [ $? -ne 0 ]; then\n\
                  \    echo 'Command failed: %S'\n\
                  \    %s register-failure %ld 'shell_command_failed %S'\n\
                  %sfi\n"
            s s s hitscore_command b2f.Layout.Function_bcl_to_fastq.id s
            (Option.value_map ~default:"" if_ok
               ~f:(fun sl -> sprintf "  else\n%s\n"
                 (String.concat ~sep:"\n  " sl)))
        in
        Sequme_pbs.(make_script
                      ~mail_options:[JobAborted; JobBegun; JobEnded]
                      ~user_list:[user ^ "@nyu.edu"]
                      ~resource_list
                      ~job_name
                      ~stdout_path ~stderr_path ~queue [
                        ksprintf checked_command 
                          "echo $PBS_JOBID > %s/jobid_2" run_dir;
                        ksprintf checked_command "echo %S > %s/workdir" 
                          work_root run_dir;
                        ksprintf checked_command 
                          ". /share/apps/casava/%s/intel/env.sh" casava_version;
                        ksprintf checked_command "cd %s" unaligned;
                        ksprintf checked_command "%s 1> %s 2> %s" 
                          make_command make_stdout_path make_stderr_path;
                        (let if_ok = [
                           sprintf "%s register-success %ld %s" hitscore_command
                             b2f.Layout.Function_bcl_to_fastq.id work_root] in
                         ksprintf (checked_command ~if_ok) 
                           "test `cat %s/Basecall_Stats_*/Demultiplex_Stats.htm \
                                 | wc -l` -gt 5" unaligned);
                        "echo \"Done: $(date -R)\"";
                      ]
                    |! script_to_string)
      in
      let create ~dbh =
        Layout.Function_bcl_to_fastq.(
          add_evaluation ~dbh
            ~raw_data:hiseq_dir ?tiles
            ~availability ~mismatch:mismatch32 ~version:casava_version
            ~sample_sheet ~recomputable:true ~recompute_penalty:100.
          >>= fun b2f ->
          let log =
            sprintf "(create_bcl_to_fastq %ld (raw_data %ld) \
                        (availability %ld) (mismatch %ld) (version %s) \
                        %s (sample_sheet %ld))"
              b2f.id
              hiseq_dir.Layout.Record_hiseq_raw.id
              availability.Layout.Record_inaccessible_hiseq_raw.id
              mismatch32 casava_version
              (Option.value_map ~default:"()" ~f:(sprintf "(tiles %S)") tiles)
              sample_sheet.Layout.Record_sample_sheet.id in
          Layout.Record_log.add_value ~dbh ~log
          >>= fun _ -> return b2f
        )
      in
      let start ~dbh b2f =
        Layout.Function_bcl_to_fastq.(
          set_started ~dbh b2f
          >>= fun b2f ->
          Layout.Record_log.add_value ~dbh 
            ~log:(sprintf "(set_bcl_to_fastq_started %ld)" b2f.id)
          >>= fun _ -> return b2f) in            

      let cmd fmt = ksprintf (fun s -> system_command s) fmt in 
      cmd "mkdir -p %s" out_dir >>= fun () ->
      ACL.set_defaults ~dbh (`dir work_root) ~configuration
      >>= fun () ->
      cmd ". /share/apps/casava/%s/intel/env.sh && \
                  configureBclToFastq.pl --fastq-cluster-count 800000000 \
                    %s --input-dir %s \
                    --output-dir %s \
                    --sample-sheet %s \
                    --mismatches %ld"
        casava_version 
        (Option.value_map ~default:"" ~f:(sprintf "--tiles %S") tiles)
        basecalls unaligned sample_sheet_path mismatch32
      >>= fun () ->
      create ~dbh
      >>= fun created ->
      let started =
        let pbs_script_created = pbs_script created in
        write_file pbs_script_created pbs_script_file >>= fun () ->
        ACL.set_defaults ~dbh ~configuration (`file pbs_script_file)
        >>= fun () ->
        let run_dir =
          (work_run_time work_dir created.Layout.Function_bcl_to_fastq.id) in
        cmd "mkdir -p %s" run_dir
        >>= fun () ->
        cmd "qsub %s > %s/jobid" pbs_script_file run_dir
        >>= fun () ->
        start ~dbh created
      in
      double_bind started
        ~ok:(fun o -> return (`success o))
        ~error:(fun e ->
          Layout.Function_bcl_to_fastq.set_failed ~dbh created
          >>= fun failed ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_bcl_to_fastq_failed %ld while_creating_starting)" 
                    failed.Layout.Function_bcl_to_fastq.id)
          >>= fun _ ->
          return (`failure (failed, e)))
      
    let succeed ~dbh ~configuration ~bcl_to_fastq ~result_root =
      Layout.File_system.(
        let files = Tree.([opaque "Unaligned"]) in
        add_volume ~dbh ~hr_tag:(Filename.basename result_root)
          ~kind:`bcl_to_fastq_unaligned_opaque ~files
        >>= fun vol ->
        get_volume_entry ~dbh vol
        >>= fun vol_ec ->
        return (vol, entry_unix_path vol_ec))
      >>= fun (directory, path_vol) ->
      let move_m =
        match Configuration.volume_path configuration path_vol with
        | Some vol_dir -> 
          ksprintf system_command "mkdir -p %s/" vol_dir >>= fun () ->
          ksprintf system_command "mv %s/* %s/" result_root vol_dir >>= fun () ->
          ACL.set_defaults ~dbh (`dir vol_dir) ~configuration
        | None -> error `root_directory_not_configured
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
            get_volume ~dbh directory
            >>= fun cache ->
            delete_volume ~dbh cache
            >>= fun () ->
            return (sexp_of_volume cache))
          >>= fun sexp ->
          Layout.Function_bcl_to_fastq.set_failed ~dbh bcl_to_fastq
          >>= fun failed ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(delete_orphan_volume %s)"
                    (Sexplib.Sexp.to_string_hum sexp))
          >>= fun _ ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_bcl_to_fastq_failed %ld)" 
                    failed.Layout.Function_bcl_to_fastq.id)
          >>= fun _ ->
          return (`failure (failed, e)))

    let fail ~dbh ?reason bcl_to_fastq =
      Layout.Function_bcl_to_fastq.set_failed ~dbh bcl_to_fastq
      >>= fun failed ->
      Layout.Record_log.add_value ~dbh
        ~log:(sprintf "(set_bcl_to_fastq_failed %ld%s)" 
                failed.Layout.Function_bcl_to_fastq.id
                (Option.value_map ~default:"" reason
                   ~f:(sprintf " (reason %S)")))
      >>= fun _ ->
      return failed


    let status ~dbh ~configuration bcl_to_fastq = 
      begin match Configuration.work_directory configuration with
      | Some work_dir -> return work_dir
      | None -> error `work_directory_not_configured
      end
      >>= fun work_dir ->
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq 
        >>= fun { g_status; _ } ->
        match g_status with
        | `Started ->
          let run_dir = work_run_time work_dir bcl_to_fastq.id in
          ksprintf system_command "cat %s/jobid && qstat `cat %s/jobid`" 
            run_dir run_dir
          |! double_bind
              ~ok:(fun () -> return (`running))
              ~error:(fun e -> return (`started_but_not_running e))
        | s -> return (`not_started s)
      )

    let kill ~dbh ~configuration  bcl_to_fastq = 
      begin match Configuration.work_directory configuration with
      | Some work_dir -> return work_dir
      | None -> error `work_directory_not_configured
      end
      >>= fun work_dir ->
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq 
        >>= fun { g_status } ->
        match g_status  with
        | `Started ->
          let run_dir = work_run_time work_dir bcl_to_fastq.id in
          ksprintf system_command "cat %s/jobid && qdel `cat %s/jobid`" 
            run_dir run_dir
          >>= fun () ->
          fail ~dbh ~reason:"killed" bcl_to_fastq
        | s -> error (`not_started s)
      )



  end
