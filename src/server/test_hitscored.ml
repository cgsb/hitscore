open Hitscored_std
  
let syscmd fmt =
  let f s =
    dbg "SYSCMD: %s" s >>= fun () ->
    system_command s in
  ksprintf f fmt

let main ~server_cert ?with_auth () =
  debug "Starting" >>= fun () ->
  syscmd "_build/src/server/hitscored %s %s &"
    server_cert Option.(value_map ~default:"" ~f:fst with_auth)
  >>= fun ()-> 
  Flow_net.sleep 1. >>= fun () ->
  debug "Started server" >>= fun () ->
  Flow_net.ssl_client_context ~verification_policy:`ok_self_signed
    Option.(value_map with_auth ~default:`anonymous
              ~f:(fun (_, c) -> `with_pem_certificate c))
  >>= fun ssl_context ->
  let socket =
    Lwt_unix.(
      try
      let fd = socket PF_INET SOCK_STREAM 0 in
      fd
      with
      | Unix.Unix_error (e, s, a) as ex ->
        eprintf "Unix.Unix_error: %s %s %s\n%!" (Unix.error_message e) s a;
        raise ex
    ) in
  wrap_io (Lwt_unix.connect socket) Unix.(ADDR_INET (inet_addr_loopback, 2000))
  >>= fun () -> 
  debug "Connected (unix)" >>= fun () ->
  Flow_net.ssl_connect socket ssl_context >>= fun socket ->
  let inchan = Lwt_ssl.in_channel_of_descr  socket in
  let ouchan = Lwt_ssl.out_channel_of_descr socket in
  debug "Connected (ssl), writing" >>= fun () ->
  Message.client_send ouchan `hello
  >>= fun () ->
  Message.recv_server inchan
    >>= fun message ->
  begin match message with
  | `hello `anonymous ->
    dbg "I'm anonymous"
  | `hello (`authenticated roles) ->
    dbg "I'm authenticated, roles: [%s]"
      (List.map roles Layout.Enumeration_role.to_string |! String.concat ~sep:", ")
  end
  >>= fun () ->
  Flow_net.ssl_shutdown socket >>= fun () ->
  debug "Disconnected (ssl)" >>= fun () ->
  Flow_net.sleep 2. >>= fun () ->
  return ()


let () =
  Ssl.init ();
  global_log_app_name := "TEST";
  let main_m =
    let with_auth =
      if Array.length Sys.argv = 4
      then Some (Sys.argv.(2), Sys.argv.(3)) else None in
    double_bind (main ~server_cert:Sys.argv.(1) ?with_auth ()) ~ok:return
      ~error:(fun e ->
        system_command "killall hitscored" >>= fun () ->
        error e) in
  match Lwt_main.run main_m with
  | Ok () -> ()
  | Error e ->
    eprintf "%s: There were uncaught errors given to Lwt_main.run\n%!"
      !global_log_app_name;
    print_error e
  
