(** Posix ACLs management.  *)
module type ACL = sig

  (**/**)

  (** Local definition of the configuration. *)
  module Configuration : Hitscore_interfaces.CONFIGURATION

  (** Local definition of Result_IO.  *)
  module Result_IO : Hitscore_interfaces.RESULT_IO

  (** Local definition of Layout *)
  module Layout : Hitscore_layout_interface.LAYOUT

  (**/**)
    
  (** 
      Set the default ACLs for {ul
        {li the configuration {i group} and {i writers}}
        {li the [`administrator] and [`auditor] roles in the Layout}
      }
      {b Note:} Calling [set_defaults] with [`dir "some/path"] is
      aggressive and recursive (potentially one [chown] and 4
      [find]s). *)
  val set_defaults :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    [ `dir of string | `file of string ] ->
    (unit, [> `layout_inconsistency of
        [> `record_log | `record_person ] *
          [> `insert_did_not_return_one_id of string * int32 list
          | `select_did_not_return_one_tuple of string * int ]
           | `pg_exn of exn
           | `system_command_error of string * exn ]) Result_IO.monad
end

module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO)
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t):
  ACL
  with module Configuration = Configuration
  with module Result_IO = Result_IO
  with module Layout = Layout
= struct

    module Result_IO = Result_IO
    module Configuration = Configuration
    module Layout = Layout

    open Hitscore_std
    open Result_IO
      

    let get_people role ~dbh =
      Layout.Record_person.(
        get_all ~dbh >>= fun citizens ->
        of_list_sequential citizens ~f:(fun k ->
          get ~dbh k >>= function
          | {g_id; login = Some l; roles} when Array.exists roles ~f:((=) role) ->
            return (Some l)
          | _ -> return None))
      >>= fun vips ->
      return (List.filter_opt vips)

    let set_defaults ~dbh ~configuration =
      let runned_commands = ref [] in (* just for logging puposes *)
      let cmd fmt = 
        ksprintf (fun s -> 
          runned_commands := s :: !runned_commands;
          system_command s) fmt in
      let try_login login =
        cmd "groups %s" login |! double_bind
            ~ok:(fun () -> return (Some login))
            ~error:(fun _ -> return None)
      in
      let log_commands f =
        let cmds = List.map !runned_commands ~f:(sprintf "%S")
                   |! String.concat ~sep:" " in
        runned_commands := [];
        Layout.Record_log.add_value ~dbh ~log:(f cmds)
        >>= fun _ -> return ()
      in
      function
      | `dir root -> 
        begin match (Configuration.root_group configuration) with
        | None -> return None
        | Some grp -> 
          cmd "chown -R :%s %s" grp root 
          >>= fun () ->
          cmd "find %s -type d -exec chmod u+rwx,g-rw,g+xs,o-rwx {} \\;" root
          >>= fun () ->
          cmd "find %s -type f -exec chmod u+rwx,g-rwx,o-rwx {} \\;" root
          >>= fun () ->
          log_commands (sprintf "(chmod_group_dir %s %s (commands %s))" grp root)
          >>= fun () ->
          return (Some grp)
        end
        >>= fun group ->
        let configured_writers = Configuration.root_writers configuration in
        get_people ~dbh `administrator >>= fun admins ->
        of_list_sequential admins try_login >>| List.filter_opt
        >>= fun valid_admins ->
        let valid_writers = List.dedup (valid_admins @ configured_writers) in
        get_people ~dbh `auditor >>= fun readers ->
        of_list_sequential readers try_login >>| List.filter_opt
        >>= fun valid_readers ->
        begin match valid_writers, valid_readers with
        | [], [] -> return ()
        | w, r ->
          let concat_map l f = String.concat ~sep:"" (List.map l f) in 
          cmd "find %s -type d -exec setfacl -m %s%s%s%s%sm:rwx {} \\;" root
            (concat_map r (sprintf "user:%s:rx,"))
            (concat_map r (sprintf "d:user:%s:rx,"))
            (concat_map w (sprintf "user:%s:rwx,"))
            (concat_map w (sprintf "d:user:%s:rwx,"))
            (Option.value_map ~default:"" ~f:(sprintf "g:%s:x,") group)
          >>= fun () ->
          cmd "find %s -type f -exec setfacl -m %s%sm:rwx {} \\;" root
            (concat_map r (sprintf "user:%s:r,"))
            (concat_map w (sprintf "user:%s:rw,"))
          >>= fun () ->
          log_commands (sprintf "(set_acls_on_dir %s \
                            (configured_writers %s) \
                            (role_writers %s) \
                            (valid_writers %s) \
                            (role_readers %s) \
                            (valid_readers %s) \
                            (commands %s))" root
                          (String.concat ~sep:" " configured_writers)
                          (String.concat ~sep:" " admins)
                          (String.concat ~sep:" " valid_writers)
                          (String.concat ~sep:" " readers)
                          (String.concat ~sep:" " valid_readers))
        end
      | `file f ->
        begin match (Configuration.root_group configuration) with
        | None -> return ()
        | Some grp -> 
          cmd "chown :%s %s" grp f >>= fun () ->
          cmd "chmod 0600 %s" f
          >>= fun () ->
          log_commands (sprintf "(chmod_group_file %s %s (commands %s))" grp f)
        end
        >>= fun () ->
        let configured_writers = Configuration.root_writers configuration in
        get_people ~dbh `administrator >>= fun admins ->
        of_list_sequential admins try_login >>| List.filter_opt
        >>= fun valid_admins ->
        let valid_writers = List.dedup (valid_admins @ configured_writers) in
        get_people ~dbh `auditor >>= fun readers ->
        of_list_sequential readers try_login >>| List.filter_opt
        >>= fun valid_readers ->
        begin match valid_writers, valid_readers with
        | [], [] -> return ()
        | w, r ->
          let concat_map l f = String.concat ~sep:"" (List.map l f) in 
          cmd "setfacl -m %s%sm:rwx %s"
            (concat_map r (sprintf "user:%s:r,"))
            (concat_map w (sprintf "user:%s:rw,")) f
          >>= fun () ->
          log_commands (sprintf "(set_acls_on_file %s \
                            (configured_writers %s) \
                            (role_writers %s) \
                            (valid_writers %s) \
                            (role_readers %s) \
                            (valid_readers %s) \
                            (commands %s))" f
                          (String.concat ~sep:" " configured_writers)
                          (String.concat ~sep:" " admins)
                          (String.concat ~sep:" " valid_writers)
                          (String.concat ~sep:" " readers)
                          (String.concat ~sep:" " valid_readers))
        end




end
