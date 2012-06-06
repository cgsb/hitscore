(** Container-module to make it easier to pass arguments to functors. *)

open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_configuration
  

(** Container-module to make it easier to pass arguments to functors. *)
module type COMMON = sig

  (** Add a log to the database. *)
  val add_log :
    dbh:Backend.db_handle ->
    string ->
    (unit,
     [> `Layout of
         Hitscore_layout.Layout.error_location *
           Hitscore_layout.Layout.error_cause ]) Flow.monad
           
  (** Check that an HiSeq-raw directory (the database) record is usable. *) 
  val check_hiseq_raw_availability :
    dbh:Backend.db_handle ->
    hiseq_raw:Layout.Record_hiseq_raw.pointer ->
    (Layout.Record_inaccessible_hiseq_raw.pointer *
       Layout.Record_hiseq_raw.pointer Hitscore_std.List.t,
     [> `Layout of
         Hitscore_layout.Layout.error_location *
           Hitscore_layout.Layout.error_cause 
     | `hiseq_dir_deleted ]) Flow.monad

  (** Get the full path from an HiSeq directory name. *)
  val hiseq_raw_full_path:
    configuration:Configuration.local_configuration ->
    string -> (string, [> `raw_data_path_not_configured]) Flow.monad
    
  (** Convert a list of file-system trees to unix paths relative to a
      volume. *)
  val trees_to_unix_relative_paths:
    Layout.File_system.tree list -> string list

  (** Get the directory corresponding to a given volume (i.e. something
      like ["vol/0000042_HumanReadable"]). *)
  val volume_unix_directory:
    ?hr_tag: string ->
    id:int ->
    kind:Layout.Enumeration_volume_kind.t ->
    string
      
  (** Get the trees of a volume pointer (follows virtual symbolic links). *)
  val trees_of_volume :
    dbh:Backend.db_handle ->
    configuration:'a ->
    Layout.File_system.pointer ->
    (Layout.File_system.tree list,
     [> `Layout of
         Hitscore_layout.Layout.error_location *
           Hitscore_layout.Layout.error_cause ]) Flow.monad
      
  (** Get all the full paths of a given volume pointer (follows
      virtual symbolic links). *)
  val path_of_volume :
    configuration:Configuration.local_configuration ->
    dbh:Backend.db_handle ->
    Layout.File_system.pointer ->
    (string,
     [> `Layout of
         Hitscore_layout.Layout.error_location *
           Hitscore_layout.Layout.error_cause
     | `root_directory_not_configured ])
      Flow.monad

  (** Get all the full paths of a given volume pointer (follows
      virtual symbolic links). *)
  val all_paths_of_volume :
    configuration:Configuration.local_configuration ->
    dbh:Backend.db_handle ->
    Layout.File_system.pointer ->
    (string Hitscore_std.List.t,
     [> `Layout of
         Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
     | `root_directory_not_configured ]) Flow.monad

  module PBS: sig

    type t
    type t_fun = int -> t

    (** [make "B2F"] Creates a PBS management environment where the
        run/work directories are tagged with ["B2F"]. *)
    val make : ?pbs_save_directory:string -> ?pbs_script_filename:string ->
      string -> t_fun

    val work_path_for_job :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Flow.monad
    val pbs_output_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Flow.monad
    val pbs_runtime_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Flow.monad
    val pbs_result_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Flow.monad
    val pbs_script_path :
      t ->
      configuration:Configuration.local_configuration ->
      (string, [> `work_directory_not_configured ]) Flow.monad
    val prepare_work_environment :
      t ->
      dbh:Backend.db_handle ->
      configuration:Configuration.local_configuration ->
      (unit,
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause
       | `system_command_error of string * exn
       | `work_directory_not_configured ])
        Flow.t

    val save_pbs_runtime_information :
      t ->
      dbh:Backend.db_handle ->
      configuration:Configuration.local_configuration ->
      string ->
      (unit,
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause
       | `system_command_error of string * exn
       | `work_directory_not_configured ])
        Flow.t

    (** Create a PBS script. *)
    val pbs_script :
      t ->
      configuration:Configuration.local_configuration ->
      nodes:int ->
      ppn:int ->
      wall_hours:int ->
      ?queue:string ->
      ?user:string ->
      job_name:string ->
      on_command_failure:(string -> string) ->
      add_commands:(checked:(string -> unit) ->
                    non_checked:(string -> unit) -> 'a) ->
      (string, [> `work_directory_not_configured ]) Flow.monad

    (** Write a PBS script to a file, set the access rights, and qsub it. *)
    val qsub_pbs_script :
      t ->
      dbh:Backend.db_handle ->
      configuration:Configuration.local_configuration ->
      string ->
      (unit,
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause
       | `system_command_error of string * exn
       | `work_directory_not_configured
       | `write_file_error of string * string * exn ]) Flow.monad
        
    val qstat :
      t ->
      configuration:Configuration.local_configuration ->
      ([> `running
       | `started_but_not_running of
           [> `system_command_error of string * exn ] ],
       [> `work_directory_not_configured ])
        Flow.monad

    val qdel :
      t ->
      configuration:Configuration.local_configuration ->
      (unit, 
       [> `system_command_error of string * exn
       | `work_directory_not_configured ])
        Flow.monad

  end


  module Make_pbs_function:
    functor (Layout_function : sig
      type pointer = { id : int; }
      val set_failed :
        dbh:Hitscore_db_backend.Backend.db_handle ->
        pointer ->
        (unit,
         [> `Layout of
             Hitscore_layout.Layout.error_location *
               Hitscore_layout.Layout.error_cause ]) Flow.monad
      val get_status :
        pointer ->
        dbh:Hitscore_db_backend.Backend.db_handle ->
        (Layout.Enumeration_process_status.t,
         [> `Layout of
             Hitscore_layout.Layout.error_location *
               Hitscore_layout.Layout.error_cause ]) Flow.monad

      val pbs_fun : PBS.t_fun
      val name_in_log: string
    end) ->
  sig
    
    (** Register the evaluation as failed with a optional reason to add
        to the [log] (record). *)
    val fail :
      dbh:Hitscore_db_backend.Backend.db_handle ->
      ?reason:string ->
      Layout_function.pointer ->
      (Layout_function.pointer,
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause ]) Flow.monad
        
    (** Get the status of the evaluation by checking its data-base
        status and it presence in the PBS queue. *)
    val status :
      dbh:Hitscore_db_backend.Backend.db_handle ->
      configuration:Configuration.local_configuration ->
      Layout_function.pointer ->
      ([> `not_started of Layout.Enumeration_process_status.t
       | `running
       | `started_but_not_running of
           [> `system_command_error of string * exn ] ],
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause
       | `work_directory_not_configured ]) Flow.monad

    (** Kill the evaluation ([qdel]) and set it as failed. *)
    val kill :
      dbh:Backend.db_handle ->
      configuration:Configuration.local_configuration ->
      Layout_function.pointer ->
      (Layout_function.pointer,
       [> `Layout of
           Hitscore_layout.Layout.error_location *
             Hitscore_layout.Layout.error_cause
       | `not_started of
           Hitscore_layout.Layout.Enumeration_process_status.t
       | `system_command_error of string * exn
       | `work_directory_not_configured ])
        Flow.monad
  end

end


module Common : COMMON = struct

  let add_log ~dbh log = 
    Access.Log.(
      add_value ~dbh ~log >>= fun _ ->
      return ())

  let check_hiseq_raw_availability ~dbh ~hiseq_raw =
    Layout.Record_inaccessible_hiseq_raw.(
      Access.Inaccessible_hiseq_raw.get_all ~dbh >>= fun all ->
      of_list_sequential all ~f:(fun t ->
        let p = pointer t in
        return (p, t.g_last_modified, Array.to_list t.g_value.deleted))
      >>| List.sort ~cmp:(fun a b -> compare (snd3 b) (snd3 a))
      >>=
        (function
        | [] ->
          printf "There were no inaccessible_hiseq_raw => \
                       creating the empty one.\n";
          Access.Inaccessible_hiseq_raw.add_value ~dbh ~deleted:[| |]
          >>= fun pointer ->
          return (pointer, [])
        | (h, ts, l) :: t as whole-> 
          printf "Last inaccessible_hiseq_raw: %d on %s\n"
            h.Layout.Record_inaccessible_hiseq_raw.id
            (Time.to_string ts);
          return (h, List.map whole trd3 |! List.concat))
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
    sprintf "%s/%09d%s" toplevel id
      (Option.value_map ~default:"" hr_tag ~f:((^) "_"))

  let rec trees_of_volume ~dbh ~configuration volume_pointer =
    Layout.File_system.(
      Access.Volume.get ~dbh volume_pointer >>= fun {g_content} ->
      match g_content with
      | Link p -> trees_of_volume ~dbh ~configuration p
      | Tree (_, t) -> return t)


  let rec path_of_volume ~configuration ~dbh volume_pointer =
    let open Layout.File_system in
    Access.Volume.get ~dbh volume_pointer
    >>= fun { g_id = id; g_kind; g_content } ->
    match g_content with
    | Tree (hr_tag, trees) ->
      let vol = volume_unix_directory ~id ~kind:g_kind ?hr_tag in
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
    Access.Volume.get ~dbh volume_pointer
    >>= fun { g_id = id; g_kind; g_content } ->
    match g_content with
    | Tree (hr_tag, trees) ->
      let relative_paths = trees_to_unix_relative_paths trees in
      let vol = volume_unix_directory ~id ~kind:g_kind ?hr_tag in
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
      id : int;
      pbs_script_filename: string;
      pbs_save_directory: string;
    }
    type t_fun = int -> t
      
    let make ?(pbs_save_directory="_pbs_run")
        ?(pbs_script_filename="script.pbs") tag =
      fun id -> { tag; id; pbs_script_filename; pbs_save_directory; }

    let work_path_for_job t ~configuration =
      begin match Configuration.work_path configuration with
      | Some work_path -> return work_path
      | None -> error `work_directory_not_configured
      end
      >>= fun work_path ->
      return Filename.(concat work_path (sprintf "%s_%d" t.tag t.id))

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
        
    let prepare_work_environment t ~dbh ~configuration  =
      work_path_for_job t ~configuration  >>= fun wp ->
      pbs_output_path   t ~configuration  >>= fun op ->
      pbs_runtime_path  t ~configuration  >>= fun rp ->
      pbs_result_path   t ~configuration  >>= fun sp ->
      ksprintf system_command "mkdir -p %s" wp >>= fun () -> 
      ksprintf system_command "mkdir -p %s" op >>= fun () -> 
      ksprintf system_command "mkdir -p %s" rp >>= fun () -> 
      ksprintf system_command "mkdir -p %s" sp >>= fun () -> 
      Access_rights.set_posix_acls ~dbh (`dir wp) ~configuration >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir op) ~configuration >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir rp) ~configuration >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir sp) ~configuration
        
    let save_pbs_runtime_information t ~dbh ~configuration dest =
      pbs_output_path   t ~configuration  >>= fun op ->
      pbs_runtime_path  t ~configuration  >>= fun rp ->
      pbs_script_path   t ~configuration  >>= fun sp ->
      let dest_path = Filename.concat dest t.pbs_save_directory in
      ksprintf system_command "mkdir -p %s/" dest_path >>= fun () ->
      ksprintf system_command "mv %s %s %s %s/" op rp sp dest_path >>= fun () ->
      Access_rights.set_posix_acls ~dbh (`dir dest_path) ~configuration


        
    let pbs_script t ~configuration 
        ~nodes ~ppn ~wall_hours ?queue ?user ~job_name
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
      let user_list =
        Option.value_map ~default:[] user ~f:(fun u -> [u ^ "nyu.edu"]) in
      return Sequme_pbs.(make_script
                           ~mail_options:[JobAborted; JobBegun; JobEnded]
                           ~user_list
                           ~resource_list
                           ~job_name
                           ~stdout_path ~stderr_path ?queue commands
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


  module Make_pbs_function (Layout_function : sig
    type pointer = { id : int; }
      val set_failed :
        dbh:Hitscore_db_backend.Backend.db_handle ->
        pointer ->
        (unit,
         [> `Layout of
             Hitscore_layout.Layout.error_location *
               Hitscore_layout.Layout.error_cause ]) Flow.monad
      val get_status :
        pointer ->
        dbh:Hitscore_db_backend.Backend.db_handle ->
        (Layout.Enumeration_process_status.t,
         [> `Layout of
             Hitscore_layout.Layout.error_location *
               Hitscore_layout.Layout.error_cause ]) Flow.monad
    val pbs_fun : PBS.t_fun
    val name_in_log: string
  end) = struct

    let fail ~dbh ?reason f =
      Layout_function.set_failed ~dbh f
      >>= fun () ->
      Access.Log.add_value ~dbh
        ~log:(sprintf "(set_%s_failed %d%s)" Layout_function.name_in_log
                f.Layout_function.id
                (Option.value_map ~default:"" reason
                   ~f:(sprintf " (reason %S)")))
      >>= fun _ ->
      return f

    let status ~dbh ~configuration pointer = 
      Layout_function.(
        get_status ~dbh pointer  
        >>= function
        | `Started ->
          PBS.qstat (Layout_function.pbs_fun pointer.Layout_function.id) ~configuration
        | s -> return (`not_started s))

    let kill ~dbh ~configuration  f = 
      Layout_function.(
        get_status ~dbh f 
        >>= function
        | `Started ->
          PBS.qdel (Layout_function.pbs_fun f.id) ~configuration >>= fun () ->
          fail ~dbh ~reason:"killed" f
        | s -> error (`not_started s))

  end

end
