
module Make
  (Result_IO : Hitscore_result_IO.RESULT_IO) 
  (Layout: module type of Hitscore_db_access.Make(Result_IO)) = struct

    open Hitscore_std
    open Result_IO

    let start ~dbh ~root
        ~(sample_sheet: Layout.Record_sample_sheet.t)
        ~(hiseq_dir: Layout.Record_hiseq_raw.t)
        ~(availability: Layout.Record_inaccessible_hiseq_raw.t)
        ?(mismatch=`one)
        ?(version=`casava_182)
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(work_dir=fun ~user ~unique_id -> 
          sprintf "/scratch/%s/_HS_B2F/%s" user unique_id)
        ?(queue="cgsb-s")
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
            return (sprintf "%s/%s/%s" root vol one)
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
      let work_root = work_dir ~user ~unique_id in
      let out_dir = sprintf "%s/Output/" work_root in
      let unaligned = sprintf "%s/Unaligned" work_root in
      let pbs_script_file = sprintf "%s/script.pbs" work_root in
      let pbs_script b2f = 
        let stdout_path = sprintf "%s/pbs.stdout" out_dir in
        let stderr_path = sprintf "%s/pbs.stderr" out_dir in
        let make_stdout_path = sprintf "%s/make.stdout" out_dir in
        let make_stderr_path = sprintf "%s/make.stderr" out_dir in
        let resource_list =
          sprintf "%swalltime=%d:00:00\n"
            (match nodes, ppn with
            | 0, 0 -> ""
            | n, m -> sprintf "nodes=%d:ppn=%d," n m)
            wall_hours in
        let job_name = sprintf "HS-B2F-%s" name in
        Sequme_pbs.(make_script
                      ~mail_options:[JobAborted; JobBegun; JobEnded]
                      ~user_list:[user ^ "@nyu.edu"]
                      ~resource_list
                      ~job_name
                      ~stdout_path ~stderr_path ~queue
                      [sprintf 
                          ". /share/apps/casava/%s/intel/env.sh" casava_version;
                       sprintf "cd %s" unaligned;
                       sprintf "make -j8 1> %s 2> %s" 
                         make_stdout_path make_stderr_path;
                       sprintf "echo \"hitscore dev b2f register-success %ld %s\""
                         b2f.Layout.Function_bcl_to_fastq.id work_root]
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
      cmd "mkdir -p %s" out_dir
      >>= fun () ->
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
      write_file (pbs_script created) pbs_script_file
      >>= fun () ->
      cmd "qsub %s" pbs_script_file
      >>= fun () ->
      start ~dbh created


    let finish ~dbh = ()

    let status ~dbh = ()

  end
