
module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (ACL : Hitscore_interfaces.ACL 
         with module RIO = Result_IO
         with module Config = Configuration)
  (Layout: module type of Hitscore_db_access.Make(Result_IO)) = struct

    open Hitscore_std
    open Result_IO

    type 'a start_error = 
    [> `cannot_recognize_file_type of string
    | `empty_sample_sheet_volume of
        Layout.File_system.volume * Layout.Record_sample_sheet.t
    | `hiseq_dir_deleted of
        Layout.Record_hiseq_raw.t * Layout.Record_inaccessible_hiseq_raw.t
    | `inconsistency_inode_not_found of int32
    | `layout_inconsistency of
        [> `file_system
        | `function_bcl_to_fastq
        | `record_hiseq_raw
        | `record_inaccessible_hiseq_raw
        | `record_log
        | `record_sample_sheet ] *
          [> `insert_did_not_return_one_id of string * int32 list
          | `select_did_not_return_one_cache of string * int ]
    | `more_than_one_file_in_sample_sheet_volume of
        Layout.File_system.volume * Layout.Record_sample_sheet.t * string list
    | `pg_exn of exn 
    | `root_directory_not_configured
    | `work_directory_not_configured
    ] as 'a

    let work_root_directory work_dir unique_id =
      sprintf "%s/B2F/work/%s/" work_dir unique_id

    let work_run_time work_dir db_id =
      sprintf "%s/B2F/run/%ld" work_dir db_id

    let start ~dbh ~configuration
        ~(sample_sheet: Layout.Record_sample_sheet.t)
        ~(hiseq_dir: Layout.Record_hiseq_raw.t)
        ~(availability: Layout.Record_inaccessible_hiseq_raw.t)
        ?(mismatch=`one)
        ?(version=`casava_182)
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(queue="cgsb-s")
        ?(hitscore_command="echo hitscore should: ")
        ?(make_command="make -j8")
        ~run_command
        ~write_file
        name =

      Layout.Record_inaccessible_hiseq_raw.(
        cache_value ~dbh availability >>| get_fields
        >>= fun {deleted} ->
        if Array.exists deleted ((=) hiseq_dir) then
          error (`hiseq_dir_deleted (hiseq_dir, availability))
        else
          return ())
      >>= fun () ->
      Layout.Record_sample_sheet.(
        cache_value ~dbh sample_sheet >>| get_fields
        >>= fun { file; _ } ->
        Layout.File_system.(
          cache_volume ~dbh file
          >>= fun vc -> (volume_trees vc |! IO.return) >>| trees_to_unix_paths 
          >>= function
          | [one] ->
            let vol =
              volume_entry_cache vc |! volume_entry |! entry_unix_path in
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
        cache_value ~dbh hiseq_dir >>| get_fields
        >>= fun {hiseq_dir_name; _} ->
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
            ~raw_data:hiseq_dir
            ~availability ~mismatch:mismatch32 ~version:casava_version
            ~sample_sheet ~recomputable:true ~recompute_penalty:100.
          >>= fun b2f ->
          let log =
            sprintf "(create_bcl_to_fastq %ld (raw_data %ld) \
                        (availability %ld) (mismatch %ld) (version %s) \
                        (sample_sheet %ld))"
              b2f.id
              hiseq_dir.Layout.Record_hiseq_raw.id
              availability.Layout.Record_inaccessible_hiseq_raw.id
              mismatch32 casava_version
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

      let cmd fmt = ksprintf (fun s -> run_command s) fmt in 
      cmd "mkdir -p %s" out_dir >>= fun () ->
      ACL.set_defaults (`dir work_root) ~configuration ~run_command >>= fun () ->
      cmd ". /share/apps/casava/%s/intel/env.sh && \
                  configureBclToFastq.pl --fastq-cluster-count 800000000 \
                    --input-dir %s \
                    --output-dir %s \
                    --sample-sheet %s \
                    --mismatches %ld"
        casava_version basecalls unaligned sample_sheet_path mismatch32
      >>= fun () ->
      create ~dbh
      >>= fun created ->
      let started =
        let pbs_script_created = pbs_script created in
        write_file pbs_script_created pbs_script_file >>= fun () ->
        ACL.set_defaults ~run_command ~configuration (`file pbs_script_file)
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


    type 'a succeed_error = 'a constraint 'a =
    [> `layout_inconsistency of
        [> `file_system
        | `record_bcl_to_fastq_unaligned
        | `record_log ] *
          [> `add_did_not_return_one of string * int32 list
          | `insert_did_not_return_one_id of string * int32 list
          | `select_did_not_return_one_cache of string * int ]
    | `pg_exn of exn
    | `root_directory_not_configured]

    let succeed ~dbh ~configuration ~bcl_to_fastq ~result_root ~run_command =
      Layout.File_system.(
        let files = Tree.([opaque "Unaligned"]) in
        add_volume ~dbh ~hr_tag:(Filename.basename result_root)
          ~kind:`bcl_to_fastq_unaligned_opaque ~files
        >>= fun vol ->
        cache_volume_entry ~dbh vol
        >>= fun vol_ec ->
        return (vol, entry_unix_path (volume_entry vol_ec)))
      >>= fun (directory, path_vol) ->
      let move_m =
        match Configuration.volume_path configuration path_vol with
        | Some vol_dir -> 
          ksprintf run_command "mkdir -p %s/" vol_dir >>= fun () ->
          ksprintf run_command "mv %s/* %s/" result_root vol_dir >>= fun () ->
          ACL.set_defaults (`dir vol_dir) ~configuration ~run_command
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
            cache_volume ~dbh directory
            >>= fun cache ->
            delete_cache ~dbh cache
            >>= fun () ->
            return (sexp_of_volume_cache cache))
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


    let status ~dbh ~configuration ~run_command bcl_to_fastq = 
      begin match Configuration.work_directory configuration with
      | Some work_dir -> return work_dir
      | None -> error `work_directory_not_configured
      end
      >>= fun work_dir ->
      Layout.Function_bcl_to_fastq.(
        cache_evaluation ~dbh bcl_to_fastq 
        >>= fun cache ->
        match (is_started cache) with
        | Ok cache ->
          let run_dir = work_run_time work_dir bcl_to_fastq.id in
          ksprintf run_command "cat %s/jobid && qstat `cat %s/jobid`" 
            run_dir run_dir
          |! double_bind
              ~ok:(fun () -> return (`running))
              ~error:(fun e -> return (`started_but_not_running e))
        | Error (`status_parsing_error o) -> error (`status_parsing_error o)
        | Error (`wrong_status s) -> return (`not_started s)
      )


    type 'a kill_error = 'a constraint 'a =
    [> `layout_inconsistency of
        [> `function_bcl_to_fastq | `record_log ] *
          [> `insert_did_not_return_one_id of
              string * int32 list
          | `select_did_not_return_one_cache of
              string * int ]
    | `not_started of
        Layout.Enumeration_process_status.t
    | `pg_exn of exn
    | `status_parsing_error of string
    | `work_directory_not_configured ]

    let kill ~dbh ~configuration ~run_command bcl_to_fastq = 
      begin match Configuration.work_directory configuration with
      | Some work_dir -> return work_dir
      | None -> error `work_directory_not_configured
      end
      >>= fun work_dir ->
      Layout.Function_bcl_to_fastq.(
        cache_evaluation ~dbh bcl_to_fastq 
        >>= fun cache ->
        match (is_started cache) with
        | Ok cache ->
          let run_dir = work_run_time work_dir bcl_to_fastq.id in
          ksprintf run_command "cat %s/jobid && qdel `cat %s/jobid`" 
            run_dir run_dir
          >>= fun () ->
          fail ~dbh ~reason:"killed" bcl_to_fastq
        | Error (`status_parsing_error o) -> error (`status_parsing_error o)
        | Error (`wrong_status s) -> error (`not_started s)
      )



  end
