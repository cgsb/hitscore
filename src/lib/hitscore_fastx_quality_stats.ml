

module Make
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.FASTX_QUALITY_STATS
  with module Common = Common

  = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Result_IO

    module LFQS = Layout.Function_fastx_quality_stats
      
    let pbs_fun =
      Common.PBS.make ~pbs_script_filename:"fastx_quality_stats.pbs" "fxqs"

    let call_fastx ~dbh ~configuration ~volume ~option_Q file_path destdir =
      Common.path_of_volume ~dbh ~configuration volume
      >>= fun prefix ->
      begin match String.chop_prefix file_path ~prefix with
      | Some s ->
        return Filename.(if is_relative s then s else concat "." s)
      | None -> error (`file_path_not_in_volume (file_path, volume))
      end
      >>= fun relative_path ->
      let full_dest_path =
        Filename.(concat destdir (dirname relative_path)) in
      ksprintf system_command "mkdir -p %s" full_dest_path >>= fun () ->
      match Filename.(relative_path |! split_extension) with
      | chopped, (Some "gz") ->
        ksprintf system_command "gunzip -c %s \
                                 | fastx_quality_stats -Q%d -o %s.fxqs"
          file_path option_Q Filename.(concat destdir (chop_extension chopped))
      | chopped, (Some "fastq") ->
        ksprintf system_command "fastx_quality_stats -Q%d -i %s -o %s.fxqs"
           option_Q file_path Filename.(concat destdir chopped)
      | _ ->
        error (`cannot_recognize_fastq_format file_path)



    let start ~dbh ~configuration
        ?(option_Q=33l)
        ?(filter_names=["*.fastq"; "*.fastq.gz"])
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(queue="cgsb-s")
        ?(hitscore_command="echo hitscore should: ")
        (input_dir:Layout.Record_generic_fastqs.pointer)
        =
      LFQS.add_evaluation
        ~dbh ~recomputable:true ~recompute_penalty:10.
        ~input_dir ~option_Q
        ~filter_names:List.(sexp_of_t sexp_of_string filter_names
                            |! Sexp.to_string_hum)
      >>= fun created ->

      let after_start_work =
        let id = created.LFQS.id in
        let pbs = pbs_fun id in
        let job_name = sprintf "fastx" in
        LFQS.set_started ~dbh created >>= fun started ->
        Common.add_log ~dbh
          (sprintf "(started_fastx_quality_stats %ld)" id)
        >>= fun () ->
        Layout.Record_generic_fastqs.(
          get ~dbh input_dir
          >>= fun {directory} ->
          Common.all_paths_of_volume ~dbh ~configuration directory
          >>= fun all ->
          return (all, directory))
        >>= fun (possible_paths, volume) ->
        Common.PBS.pbs_result_path pbs ~configuration
        >>= fun dest_dir ->
        Common.PBS.pbs_script ~configuration pbs
          ~nodes ~ppn ~wall_hours ~queue ~user ~job_name
          ~on_command_failure:(fun cmd ->
            sprintf "%s register-failure %ld 'shell_command_failed %S'"
              hitscore_command id cmd)
          ~add_commands:(fun ~checked ~non_checked ->
            non_checked "echo \"Start: $(date -R)\"";
	    ksprintf checked ". /etc/profile.d/env-modules.sh";
            ksprintf checked "module load fastx_toolkit/intel/0.0.13";
            List.iter possible_paths (fun path_to_explore ->
              let filter_fastq =
                List.map filter_names (sprintf "-name %S")
                |! String.concat ~sep:" -o " in
              ksprintf checked "find %s %s -exec %s call-fastx %ld %ld {} %s \\;"
                path_to_explore filter_fastq
                hitscore_command volume.Layout.File_system.id option_Q dest_dir;
              ksprintf checked 
                "%s register-success %ld" hitscore_command id;
            );
            non_checked "echo \"Done: $(date -R)\"")
        >>= fun pbs_script ->
        Common.PBS.prepare_work_environment ~dbh ~configuration pbs
        >>= fun () ->
        Common.PBS.qsub_pbs_script ~dbh ~configuration pbs pbs_script
        >>= fun () ->
        return started
      in
      double_bind after_start_work
        ~ok:(fun s -> return s)
        ~error:(fun e ->
          LFQS.set_failed ~dbh created >>= fun _ ->
          error e)


    let succeed ~dbh ~configuration fxqs =
      LFQS.(get ~dbh fxqs >>= fun {g_id; input_dir; } ->
            Layout.Record_generic_fastqs.(
              get ~dbh input_dir >>= fun {directory} ->
              Common.trees_of_volume ~dbh ~configuration directory
              >>= fun trees ->
              return (input_dir.id, trees))
            >>= fun (input_id, trees) ->
            return (trees, sprintf "FXQS%ld_of_%ld" g_id input_id))
      >>= fun (files, hr_tag) ->
      Layout.File_system.(
        add_volume ~dbh ~hr_tag ~kind:`fastx_quality_stats_dir ~files
        >>= fun vol ->
        Common.path_of_volume ~dbh ~configuration vol >>= fun vol_path ->
        return (vol, vol_path))
      >>= fun (directory, path_vol) ->
      let pbs = pbs_fun fxqs.LFQS.id in
      Common.PBS.pbs_result_path pbs ~configuration >>= fun result_root ->
      Common.PBS.save_pbs_runtime_information pbs ~dbh ~configuration path_vol
      >>= fun () ->
      let move_m =
        ksprintf system_command "if [ -f %s/* ]; then mv %s/* %s; fi" result_root result_root path_vol
        >>= fun () ->
        Access_rights.set_posix_acls ~dbh (`dir path_vol) ~configuration
      in
      double_bind move_m
        ~ok:(fun () ->
          Layout.Record_fastx_quality_stats_result.add_value ~dbh ~directory
          >>= fun result ->
          LFQS.set_succeeded ~dbh ~result fxqs
          >>= fun success ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_fastx_quality_stats_succeeded %ld (result %ld))" 
                    success.LFQS.id
                    result.Layout.Record_fastx_quality_stats_result.id)
          >>= fun  _ ->
          return (`success success))
        ~error:(fun e ->
          Layout.File_system.(
            delete_volume ~dbh directory.id
            >>= fun () ->
            return (directory.id))
          >>= fun volid ->
          LFQS.set_failed ~dbh fxqs
          >>= fun failed ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(delete_orphan_volume (id %ld))" volid)
          >>= fun _ ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(set_fastx_quality_stats_failed %ld)" 
                    failed.LFQS.id)
          >>= fun _ ->
          return (`failure (failed, e)))

        
    include Common.Make_pbs_function (Layout.Function_fastx_quality_stats)
        (struct
          let pbs_fun = pbs_fun
          let name_in_log = "fastx_quality_stats"
         end)

  end
