
module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) = struct

    module Result_IO = Result_IO
    module Configuration = Configuration

    open Hitscore_std
    open Result_IO
      
    let set_defaults ~run_command ~configuration = 
      let cmd fmt = ksprintf (fun s -> run_command s) fmt in
      function
      | `dir root -> 
        begin match (Configuration.root_group configuration) with
        | None -> return ()
        | Some grp -> 
          cmd "chown -R :%s %s" grp root 
          >>= fun () ->
          cmd "find %s -type d -exec chmod u+rwx,g-rw,g+xs,o-rwx {} \\;" root
          >>= fun () ->
          cmd "find %s -type f -exec chmod u+rwx,g-rwx,o-rwx {} \\;" root
        end
        >>= fun () ->
        begin match Configuration.root_writers configuration with
        | [] -> return ()
        | l ->
          cmd "find %s -type d -exec setfacl -m %s,%s,m:rwx {} \\;" root
            (String.concat ~sep:"," (List.map l (sprintf "user:%s:rwx")))
            (String.concat ~sep:"," (List.map l (sprintf "d:user:%s:rwx")))
          >>= fun () ->
          cmd "find %s -type f -exec setfacl -m %s {} \\;" root
            (String.concat ~sep:"," (List.map l (sprintf "user:%s:rw")))
        end
      | `file f ->
        begin match (Configuration.root_group configuration) with
        | None -> return ()
        | Some grp -> 
          cmd "chown :%s %s" grp f >>= fun () ->
          cmd "chmod 0600 %s" f
        end
        >>= fun () ->
        begin match Configuration.root_writers configuration with
        | [] -> return ()
        | l ->
          cmd "setfacl -m %s %s"
            (String.concat ~sep:"," (List.map l (sprintf "user:%s:rw"))) f
        end




end
