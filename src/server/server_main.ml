

open Hitscore

open Core.Std
open Sequme_flow
open Sequme_flow_app_util

let with_profile () =
  let open Command_line.Spec in
  let default_path =
    sprintf "%s/.config/hitscore/config.sexp"
      (Option.value_exn_message "This environment has no $HOME !"
         (Sys.getenv "HOME")) in
  step (fun k profile -> k ~profile)
  ++ flag "profile" ~aliases:["-p"] (required string)
    ~doc:"<profile> The configuration profile name to use"
  ++ step (fun k configuration_file -> k ~configuration_file)
  ++ flag "configuration-file" ~aliases:["-c"]
    (optional_with_default default_path string)
    ~doc:(sprintf "<path> alternate configuration file (default %s)"
            default_path)

let init ~configuration_file ~profile ~log_file ~pid_file =

  let pid = Unix.getpid () in
  Sequme_flow_sys.write_file pid_file ~content:(Core.Pid.to_string pid)
  >>= fun () ->
  Sequme_flow_net.init_tls ();
  Sequme_flow_sys.read_file configuration_file
  >>= fun contents ->
  of_result Configuration.(
    let open Result in
    parse_str contents >>= fun c -> use_profile c profile)
  >>= fun configuration ->
  begin match log_file with
  | "-" -> Messages.init_log (`channel Lwt_io.stdout)
  | f -> Messages.init_log (`file f)
  end
  >>= fun () ->
  log "Loaded profile %S from %S" profile configuration_file >>= fun () ->
  return configuration

let handle_connection ~configuration connection =
  bind_on_error
    (Sequme_flow_io.bin_recv connection#in_channel
     >>= fun msg ->
     log "Received: %S from client." msg
     >>= fun () ->
     Sequme_flow_io.bin_send connection#out_channel (sprintf "%s back ..." msg))
    (function
    | `bin_recv (`exn e) | `bin_send (`exn e) | `io_exn e ->
      log "ERROR: (bin)I/O: %s (Ssl: %s)" (Exn.to_string e)
        (Ssl.get_error_string ())
    | `bin_recv (`wrong_length (l, s)) ->
      log "ERROR: bin-recv: `wrong_length %d" l
    | `bin_send (`message_too_long (s)) ->
      log "ERROR: bin-send: message_too_long: %s" s
    )

let start_server ~port ~configuration ~cert_key =
  let on_error = function
    | `accept_exn e ->
      log "ERROR: Accept-exception: %s" (Exn.to_string e)
    | `io_exn e ->
      log "ERROR: I/O-exception: %s" (Exn.to_string e)
    | `not_an_ssl_socket ->
      log "ERROR: Not an SSL socket"
    | `tls_accept_error e  ->
      log "ERROR: TLS-accept-exception: %s" (Exn.to_string e)
    | `bin_send  (`exn e) ->
      log "ERROR: bin-send-exception: %s" (Exn.to_string e)
    | `bin_send (`message_too_long s) ->
      log "ERROR: bin-send: message too long (%d bytes)" (String.length s)
    | `wrong_subject_format s ->
      log "ERROR: TLS: wrong_subject_format: %s" s
  in
  Sequme_flow_net.tls_server ~on_error ~port ~cert_key
    (handle_connection ~configuration) >>= fun () ->
  log "Server started on port: %d" port

    
type error = 
[ `configuration_parsing_error of exn
| `io_exn of exn
| `profile_not_found of string
| `read_file_error of string * exn
| `write_file_error of string * exn
| `socket_creation_exn of exn
| `tls_context_exn of exn ]
with sexp_of

let string_of_error e =
  Sexp.to_string_hum (sexp_of_error e)
  
    
let command =
  let open Command_line in
  let default_pid_file = "/tmp/hitscored.pid" in
  basic ~summary:"Hitscore's server application"
    Spec.(
      with_profile ()
      ++ step (fun k log_file -> k ~log_file)
      ++ flag "log-file" ~aliases:["-L"] (optional_with_default "-" string)
        ~doc:"<path> Log to file <path> (or “-” for stdout; the default)"
      ++ step (fun k pid_file -> k ~pid_file)
      ++ flag "pid-file" (optional_with_default default_pid_file string)
        ~doc:(sprintf
                "<path> write PID to a file (default: %s)" default_pid_file)
      ++ flag "cert" (required string) ~doc:"SSL certificate"
      ++ flag "key" (required string) ~doc:"SSL private key"
      ++ anon ("PORT" %: int)
    )
    (fun ~profile ~configuration_file ~log_file ~pid_file cert key port ->
      run_flow ~on_error:(fun e ->
        eprintf "End with ERRORS: %s" (string_of_error e))
        begin
          init ~profile ~configuration_file ~log_file ~pid_file
          >>= fun configuration ->
          start_server ~cert_key:(cert, key) ~port ~configuration
          >>= fun () ->
          wrap_io (fun () -> Lwt.(let (t,_) = wait () in t)) ()
          >>= fun _ ->
          return ()
        end)

    
let () =
  Command_line.(run ~version:"0" command)
    

