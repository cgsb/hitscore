open Hitscore_std
open Hitscore_layout
open Hitscore_db_backend
open Hitscore_configuration


(** Permissions management.  *)
module type ACCESS_RIGHTS = sig

  (** 
      Set the POSIX ACLs for {ul
        {li the configuration {i group} and {i writers}}
        {li the [`administrator] (“write access”) 
            and [`auditor] (“read access”) roles in the Layout}
        {li the optional [more_readers]}
      }

      The default value for [?set] and [?follow_symlinks] is [true].

      {b Note:} Calling [set_posix_acls] with [`dir "some/path"] is
      aggressive and recursive (potentially one [chown] and 4
      [find]s). *)
  val set_posix_acls :
    ?set:bool -> ?follow_symlinks:bool ->
    ?more_readers: string list ->
    dbh:Backend.db_handle ->
    configuration:Configuration.local_configuration ->
    [ `dir of string | `file of string ] ->
    (unit,
     [> `Layout of
         Hitscore_layout.Layout.error_location *
           Hitscore_layout.Layout.error_cause
     | `system_command_error of string * exn ])
      Flow.t
end

module Access_rights : ACCESS_RIGHTS = struct
             
  let get_people role ~dbh =
    let layout = Classy.make dbh in
    layout#person#all
    >>= fun citizens ->
    of_list_sequential citizens (fun someguy ->
      if Array.exists someguy#roles ~f:((=) role) then
        return someguy#login
      else
        return None)
    >>= fun vips ->
    return (List.filter_opt vips)

  let set_posix_acls ?(set=true) ?(follow_symlinks=true) ?(more_readers=[])
      ~dbh ~configuration dir_or_file =

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
      let cmds = List.rev_map !runned_commands ~f:(sprintf "%S")
                 |! String.concat ~sep:" " in
      runned_commands := [];
      Access.Log.add_value ~dbh ~log:(f cmds)
      >>= fun _ -> return ()
    in
    let find_exec what where todo =
      cmd "find %s %s -type %s -exec %s \\;"
        (if follow_symlinks then "-L" else "") where
        (match what with `dir -> "d" | `file -> "f")
        (todo "{}") in
    let setfacl what grp rl wl =
      if grp = None && rl = [] && wl = [] then 
        (sprintf "echo Nothing to do on: %s")
      else
        let concat_map l f = String.concat ~sep:"" (List.map l f) in
        sprintf "setfacl %s -m %s%s%sm:rwx %s"
          (if set then "-b" else "")
          (concat_map rl (fun u -> 
            match what with 
            | `dir -> sprintf "user:%s:rx,d:user:%s:rx," u u
            | `file -> sprintf "user:%s:r," u))
          (concat_map wl (fun u -> 
            match what with 
            | `dir -> sprintf "user:%s:rwx,d:user:%s:rwx," u u
            | `file -> sprintf "user:%s:rw," u))
          (Option.value_map ~default:"" ~f:(sprintf "g:%s:x,") grp) in
    
    let configured_writers = Configuration.root_writers configuration in
    get_people ~dbh `administrator >>= fun admins ->
    of_list_sequential admins try_login >>| List.filter_opt
    >>= fun valid_admins ->
    let valid_writers = List.dedup (valid_admins @ configured_writers) in
    get_people ~dbh `auditor >>= fun readers ->
    of_list_sequential (readers @ more_readers) try_login >>| List.filter_opt
    >>= fun valid_readers ->
    match dir_or_file with
    | `dir root -> 
      of_option (Configuration.root_group configuration) ~f:(fun grp -> 
        cmd "chown -R :%s %s" grp root 
        >>= fun () ->
        find_exec `dir root (sprintf "chmod u+rwx,g-rw,g+xs,o-rwx %s")
        >>= fun () ->
        find_exec `file root (sprintf "chmod u+rwx,g-rwx,o-rwx %s")
        >>= fun () ->
        log_commands (sprintf "(chmod_group_dir %s %s (commands %s))" grp root)
        >>= fun () ->
        return grp)
      >>= fun group ->
      find_exec `dir root (setfacl `dir group valid_readers valid_writers)
      >>= fun () ->
      find_exec `file root (setfacl `file group valid_readers valid_writers)
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
    | `file f ->
      of_option (Configuration.root_group configuration) ~f:(fun grp ->
        cmd "chown :%s %s" grp f >>= fun () ->
        cmd "chmod 0600 %s" f
        >>= fun () ->
        log_commands (sprintf "(chmod_group_file %s %s (commands %s))" grp f))
      >>= fun group ->
      cmd "%s" (setfacl `file None valid_readers valid_writers f)
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
