(** Container-module to make it easier to pass arguments to functors. *)

(** Container-module to make it easier to pass arguments to functors. *)
module type COMMON = sig

  (** Exported definition of the Configuration. *)
  module Configuration : Hitscore_interfaces.CONFIGURATION

  (** Exported definition of Result_IO.  *)
  module Result_IO : Hitscore_interfaces.RESULT_IO

  (** Exported definition of Layout. *)
  module Layout : Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
    (* This last one is used by Assemble_Sample_Sheet to call a PGSQL(dbh) *)
           
  (** Exported definition of Access_rights. *)
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
     with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout

  (** Check that an HiSeq-raw directory (the database) record is usable. *) 
  val check_hiseq_raw_availability :
    dbh:Layout.db_handle ->
    hiseq_raw:Layout.Record_hiseq_raw.pointer ->
    (Layout.Record_inaccessible_hiseq_raw.pointer *
       Layout.Record_hiseq_raw.pointer Hitscore_std.List.t,
     [> `hiseq_dir_deleted
     | `layout_inconsistency of
         [> `record_inaccessible_hiseq_raw ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `no_last_modified_timestamp of
               Layout.Record_inaccessible_hiseq_raw.pointer
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Result_IO.monad

  (** Get the full path from an HiSeq directory name. *)
  val hiseq_raw_full_path:
    configuration:Configuration.local_configuration ->
    string -> (string, [> `raw_data_path_not_configured]) Result_IO.monad
    
  (** Convert a list of file-system trees to unix paths relative to a
      volume. *)
  val trees_to_unix_relative_paths:
    Layout.File_system.tree list -> string list

  (** Get the directory corresponding to a given volume (i.e. something
      like ["vol/0000042_HumanReadable"]). *)
  val volume_unix_directory:
    ?hr_tag: string ->
    id:int32 ->
    kind:Layout.Enumeration_volume_kind.t ->
    string
      
  (** Get all the full paths of a given volume pointer. *) 
  val path_of_volume:
    configuration:Configuration.local_configuration ->
    dbh:Layout.db_handle ->
    Layout.File_system.pointer ->
    (string,
     [> `cannot_recognize_file_type of string
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `file_system ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `root_directory_not_configured ])
      Result_IO.monad

  (** Get all the full paths of a given volume pointer. *) 
  val all_paths_of_volume:
    configuration:Configuration.local_configuration ->
    dbh:Layout.db_handle ->
    Layout.File_system.pointer ->
    (string list,
     [> `cannot_recognize_file_type of string
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `file_system ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `root_directory_not_configured ])
      Result_IO.monad

  module PBS: sig

    type t
    type t_fun = int32 -> t

    val make : ?pbs_save_directory:string -> ?pbs_script_filename:string ->
      tag:string -> t_fun

    val work_path_for_job :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad
    val pbs_output_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad
    val pbs_runtime_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad
    val pbs_result_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad
    val pbs_script_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad
    val prepapre_work_environment :
      t ->
      dbh:Access_rights.Layout.db_handle ->
      configuration:Configuration.local_configuration ->
      (unit,
       [> `layout_inconsistency of
           [> `record_log | `record_person ] *
             [> `insert_did_not_return_one_id of string * int32 list
             | `select_did_not_return_one_tuple of string * int ]
       | `pg_exn of exn
       | `system_command_error of string * exn
       | `work_directory_not_configured ])
        Result_IO.monad

    val save_pbs_runtime_information :
      t ->
      dbh:Access_rights.Layout.db_handle ->
      configuration:Configuration.local_configuration ->
      string ->
      (unit,
       [> `layout_inconsistency of
           [> `record_log | `record_person ] *
             [> `insert_did_not_return_one_id of string * int32 list
             | `select_did_not_return_one_tuple of string * int ]
       | `pg_exn of exn
       | `system_command_error of string * exn
       | `work_directory_not_configured ])
        Result_IO.monad

    (** Create a PBS script. *)
    val pbs_script :
      t ->
      configuration:Configuration.local_configuration ->
      nodes:int ->
      ppn:int ->
      wall_hours:int ->
      queue:string ->
      user:string ->
      job_name:string ->
      on_command_failure:(string -> string) ->
      add_commands:(checked:(string -> unit) ->
                    non_checked:(string -> unit) -> 'a) ->
      (string, [> `work_directory_not_configured ]) Result_IO.monad

    (** Write a PBS script to a file, set the access rights, and qsub it. *)
    val qsub_pbs_script :
      t ->
      dbh:Access_rights.Layout.db_handle ->
      configuration:Configuration.local_configuration ->
      string ->
      (unit,
       [> `layout_inconsistency of
           [> `record_log | `record_person ] *
             [> `insert_did_not_return_one_id of string * int32 list
             | `select_did_not_return_one_tuple of string * int ]
       | `pg_exn of exn
       | `system_command_error of string * exn
       | `work_directory_not_configured
       | `write_file_error of string * string * exn ])
        Result_IO.monad
        
    val qstat :
      t ->
      configuration:Configuration.local_configuration ->
      ([> `running
       | `started_but_not_running of
           [> `system_command_error of string * exn ] ],
       [> `work_directory_not_configured ])
        Result_IO.monad

    val qdel :
      t ->
      configuration:Configuration.local_configuration ->
      (unit, 
       [> `system_command_error of string * exn
       | `work_directory_not_configured ])
        Result_IO.monad

  end

end


  
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
  COMMON
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout
    with module Access_rights = Access_rights
= struct
  module Configuration = Configuration
  module Result_IO = Result_IO
  module Access_rights = Access_rights
  module Layout = Layout

  open Hitscore_std
  open Result_IO

  let check_hiseq_raw_availability ~dbh ~hiseq_raw =
    Layout.Record_inaccessible_hiseq_raw.(
      get_all ~dbh >>= fun all ->
      of_list_sequential all ~f:(fun p ->
        get ~dbh p >>= fun {g_last_modified; deleted; _} ->
        begin match g_last_modified with
        | Some t -> return (p, t, Array.to_list deleted)
        | None -> error (`layout_inconsistency 
                            (`record_inaccessible_hiseq_raw,
                             `no_last_modified_timestamp p))
        end)
      >>| List.sort ~cmp:(fun a b -> compare (snd3 b) (snd3 a))
      >>=
        (function
        | [] ->
          printf "There were no inaccessible_hiseq_raw => \
                       creating the empty one.\n";
          Layout.Record_inaccessible_hiseq_raw.add_value ~dbh ~deleted:[| |]
          >>= fun pointer ->
          return (pointer, [])
        | (h, ts, l) :: t as whole-> 
          printf "Last inaccessible_hiseq_raw: %ld on %s\n"
            h.Layout.Record_inaccessible_hiseq_raw.id
            (Time.to_string ts);
          return (h, List.map whole trd3 |! List.flatten))
      >>= fun (last_avail, all_deleted) ->
      if List.exists all_deleted ((=) hiseq_raw) then
        error `hiseq_dir_deleted
      else
        return (last_avail, all_deleted))

  let hiseq_raw_full_path ~configuration dir_name =
    match Configuration.hiseq_data_path configuration with
    | Some s -> return (Filename.concat s dir_name)
    | None -> error `raw_data_path_not_configured
      
  let trees_to_unix_relative_paths trees =
    let open Layout.File_system in
    let paths = ref [] in
    let rec descent parent = function
      | File (n, _) -> paths := (parent ^ n) :: !paths
      | Opaque (n, _) -> paths := (parent ^ n ^ "/") :: !paths
      | Directory (n, _, l) -> List.iter l (descent (parent ^ n ^ "/"))
    in
    List.iter trees (descent "");
    !paths
      
  let volume_unix_directory ?hr_tag ~id ~kind =
    let toplevel = Layout.File_system.toplevel_of_kind kind in
    sprintf "%s/%09ld%s" toplevel id
      (Option.value_map ~default:"" hr_tag ~f:((^) "_"))


  let rec path_of_volume ~configuration ~dbh volume_pointer =
    let open Layout.File_system in
    get_volume ~dbh volume_pointer
    >>= fun { volume_pointer = { id }; volume_kind; volume_content } ->
    match volume_content with
    | Tree (hr_tag, trees) ->
      let vol = volume_unix_directory ~id ~kind:volume_kind ?hr_tag in
      begin match Configuration.path_of_volume_fun configuration with
      | Some vol_path ->
        return (vol_path vol)
      | None -> 
        error `root_directory_not_configured
      end
    | Link pointer ->
      path_of_volume ~configuration ~dbh pointer

  let rec all_paths_of_volume ~configuration ~dbh volume_pointer =
    let open Layout.File_system in
    get_volume ~dbh volume_pointer
    >>= fun { volume_pointer = { id }; volume_kind; volume_content } ->
    match volume_content with
    | Tree (hr_tag, trees) ->
      let relative_paths = trees_to_unix_relative_paths trees in
      let vol = volume_unix_directory ~id ~kind:volume_kind ?hr_tag in
      begin match Configuration.path_of_volume_fun configuration with
      | Some vol_path ->
        return (List.map relative_paths (Filename.concat (vol_path vol)))
      | None -> 
        error `root_directory_not_configured
      end
    | Link pointer ->
      all_paths_of_volume ~configuration ~dbh pointer

  module PBS = struct 

    type t = {
      tag : string;
      id : int32;
      pbs_script_filename: string;
      pbs_save_directory: string;
    }
    type t_fun = int32 -> t
      
    let make ?(pbs_save_directory="_pbs_run")
        ?(pbs_script_filename="script.pbs") ~tag =
      fun id -> { tag; id; pbs_script_filename; pbs_save_directory; }

    let work_path_for_job t ~configuration =
      begin match Configuration.work_path configuration with
      | Some work_path -> return work_path
      | None -> error `work_directory_not_configured
      end
      >>= fun work_path ->
      return Filename.(concat work_path (sprintf "%s_%ld" t.tag t.id))

    let pbs_output_path t ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      return Filename.(concat wp "PBS_output")

    let pbs_runtime_path t ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      return Filename.(concat wp "run")

    let pbs_result_path  t ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      return Filename.(concat wp "work")

    let pbs_script_path t ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      return Filename.(concat wp t.pbs_script_filename)
      
    let prepapre_work_environment t ~dbh ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      pbs_output_path   t ~configuration  >>= fun op ->
      pbs_runtime_path  t ~configuration  >>= fun rp ->
      pbs_result_path   t ~configuration  >>= fun sp ->
      ksprintf system_command "mkdir -p %s %s %s %s" wp op rp sp >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir wp) ~configuration
      
    let save_pbs_runtime_information t ~dbh ~configuration dest =
      pbs_output_path   t ~configuration  >>= fun op ->
      pbs_runtime_path  t ~configuration  >>= fun rp ->
      pbs_script_path   t ~configuration  >>= fun sp ->
      let dest_path = Filename.concat dest t.pbs_save_directory in
      ksprintf system_command "mkdir -p %s/" dest_path >>= fun () ->
      ksprintf system_command "mv %s %s %s %s/" op rp sp dest_path >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir dest_path) ~configuration


      
    let pbs_script t ~configuration 
        ~nodes ~ppn ~wall_hours ~queue ~user ~job_name
        ~on_command_failure ~add_commands =
      pbs_output_path t ~configuration 
      >>= fun out_path ->
      let stdout_path = sprintf "%s/pbs.stdout" out_path in
      let stderr_path = sprintf "%s/pbs.stderr" out_path in
      let resource_list =
        sprintf "%swalltime=%d:00:00\n"
          (match nodes, ppn with
          | 0, 0 -> ""
          | n, m -> sprintf "nodes=%d:ppn=%d," n m) wall_hours in
      let checked_command s =
        sprintf "echo \"$(date -R)\"\necho %S\n%s\nif [ $? -ne 0 ]; then\n\
                  \    echo 'Command failed: %S'\n\
                  \    %s\n\
                  \    exit 5\n\
                  fi\n" s s s (on_command_failure s) in
      let non_checked_command s = s in
      let commands =
        let r = ref [] in
        add_commands
          ~checked:(fun s -> r := (checked_command s) :: !r)
          ~non_checked:(fun s -> r := (non_checked_command s) :: !r);
        List.rev !r in
      return Sequme_pbs.(make_script
                           ~mail_options:[JobAborted; JobBegun; JobEnded]
                           ~user_list:[user ^ "@nyu.edu"]
                           ~resource_list
                           ~job_name
                           ~stdout_path ~stderr_path ~queue commands
                         |! script_to_string)

    let qsub_pbs_script t ~dbh ~configuration content = 
      pbs_runtime_path t ~configuration >>= fun run_path ->
      pbs_script_path t ~configuration >>= fun pbs_script_path ->
      ksprintf system_command "mkdir -p %s" run_path >>= fun () ->
      write_file ~file:pbs_script_path ~content >>= fun () ->
      Access_rights.set_posix_acls ~dbh ~configuration (`file pbs_script_path)
      >>= fun () ->
      ksprintf system_command "qsub %s > %s/jobid" pbs_script_path run_path
      
    let qstat t ~configuration =
      pbs_runtime_path t ~configuration >>= fun run_path ->
      ksprintf system_command "cat %s/jobid && qstat `cat %s/jobid`" 
        run_path run_path
      |! double_bind
          ~ok:(fun () -> return (`running))
          ~error:(function
          | `system_command_error _ as e -> return (`started_but_not_running e)
          | `work_directory_not_configured as e -> error e)

    let qdel t ~configuration =
      pbs_runtime_path t ~configuration >>= fun run_path ->
      ksprintf system_command "cat %s/jobid && qdel `cat %s/jobid`" 
        run_path run_path

          
  end

end
