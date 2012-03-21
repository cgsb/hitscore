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


  val pbs_script :
    nodes:int ->
    ppn:int ->
    wall_hours:int ->
    queue:string ->
    user:string ->
    job_name:string ->
    on_command_failure:(string -> string) ->
    work_root_path:string ->
    add_commands:(checked:(string -> unit) -> non_checked:(string -> unit) -> unit) ->
    string
      
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


  let pbs_script
      ~nodes ~ppn ~wall_hours ~queue ~user ~job_name
      ~on_command_failure ~work_root_path ~add_commands =
    let out_path = sprintf "%s/PBS_output/" work_root_path in

    let stdout_path = sprintf "%s/pbs.stdout" out_path in
    let stderr_path = sprintf "%s/pbs.stderr" out_path in

    let resource_list =
      sprintf "%swalltime=%d:00:00\n"
        (match nodes, ppn with
        | 0, 0 -> ""
        | n, m -> sprintf "nodes=%d:ppn=%d," n m)
        wall_hours in

    let checked_command s =
      sprintf "echo \"$(date -R)\"\necho %S\n%s\nif [ $? -ne 0 ]; then\n\
                  \    echo 'Command failed: %S'\n\
                  \    %s\n\
                  \    exit 5\n\
                  fi\n"
        s s s (on_command_failure s)
    in
    let non_checked_command s = s in
    let commands =
      let r = ref [] in
      add_commands
        ~checked:(fun s -> r := (checked_command s) :: !r)
        ~non_checked:(fun s -> r := (non_checked_command s) :: !r);
      List.rev !r in
    Sequme_pbs.(make_script
                  ~mail_options:[JobAborted; JobBegun; JobEnded]
                  ~user_list:[user ^ "@nyu.edu"]
                  ~resource_list
                  ~job_name
                  ~stdout_path ~stderr_path ~queue commands
                |! script_to_string)

      
end
