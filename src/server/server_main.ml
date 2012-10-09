

open Hitscore

open Core.Std
open Sequme_flow
open Sequme_flow_app_util
module Flow_net = Sequme_flow_net

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

type connection_state = {
  configuration: Configuration.local_configuration;
  connection:  Sequme_flow_net.connection;
  serialization_mode: Communication.Protocol.serialization_mode;
  user: [ `none ];
}

let recv_message state =
  Sequme_flow_io.bin_recv (Flow_net.in_channel state.connection)
  >>= fun msg ->
  let mode = state.serialization_mode in
  of_result (Communication.Protocol.up_of_string ~mode msg)

let send_message state msg =
  let mode = state.serialization_mode in
  let str =
    Communication.Protocol.string_of_down ~mode msg in
  Sequme_flow_io.bin_send (Flow_net.out_channel state.connection) str
  
let handle_up_message ~state = function
  | `log s ->
    log "Client wants to log: %S" s
    >>= fun () ->
    begin match state.user with
    | `none ->
      send_message state (`user_message "you can't!")
    end

type error_in_connection =
[ `bin_recv of [ `exn of exn | `wrong_length of int * string ]
| `bin_send of [ `exn of exn | `message_too_long of string ]
| `io_exn of exn
| `message_serialization of
    Communication.Protocol.serialization_mode * string * exn
] with sexp_of

let handle_connection_error ~state e =
  let to_log = Sexp.to_string_hum (sexp_of_error_in_connection e) in
  log "Error in connection:\n%s\nSSL: %s" to_log (Ssl.get_error_string ())
  >>= fun () ->
  Flow_net.shutdown state.connection

let handle_connection ~configuration ~mode connection =
  let state =
    { connection; configuration;
      serialization_mode = mode;
      user = `none } in
  bind_on_error
    (recv_message state
     >>= handle_up_message ~state)
    (handle_connection_error ~state)

let start_server ~port ~configuration ~cert_key ~mode =
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
    (handle_connection ~mode ~configuration) >>= fun () ->
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
      ++ Communication.Protocol.serialization_mode_flag ()
      ++ flag "cert" (required string) ~doc:"SSL certificate"
      ++ flag "key" (required string) ~doc:"SSL private key"
      ++ anon ("PORT" %: int)
    )
    (fun ~profile ~configuration_file ~log_file ~pid_file ~mode cert key port ->
      run_flow ~on_error:(fun e ->
        eprintf "End with ERRORS: %s" (string_of_error e))
        begin
          init ~profile ~configuration_file ~log_file ~pid_file
          >>= fun configuration ->
          start_server ~cert_key:(cert, key) ~port ~configuration ~mode
          >>= fun () ->
          wrap_io (fun () -> Lwt.(let (t,_) = wait () in t)) ()
          >>= fun _ ->
          return ()
        end)

    
let () =
  Command_line.(run ~version:"0" command)
    

