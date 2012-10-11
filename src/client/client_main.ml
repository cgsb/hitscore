

open Hitscore
open Core.Std
open Sequme_flow
open Sequme_flow_app_util
module Flow_net = Sequme_flow_net

module Configuration = struct
    
  type configuration_v0 = {
    host: string;
    port: int;
    auth_token: string;
  } with sexp
  type t = [
  | `gencore of [
    | `v0 of configuration_v0
  ]
  ] with sexp
    
  let make ~host ~port ~auth_token =
    `gencore (`v0 {host; port; auth_token})

  let to_string c =
    Sexp.to_string_hum (sexp_of_t c)
  let of_string s =
    Sexp.of_string s |! t_of_sexp
end
  
type connection_state = {
  connection:  Sequme_flow_net.connection;
  serialization_mode: Communication.Protocol.serialization_mode;
  user_name: string;
  configuration: Configuration.t option;
}
let state  ~mode ~user_name ?configuration connection =
  { connection; serialization_mode = mode; user_name; configuration}

let send_message ~state message =
  Sequme_flow_io.bin_send (Flow_net.out_channel state.connection)
    (Communication.Protocol.string_of_up ~mode:state.serialization_mode message)
  
let recv_message ~state =
  Sequme_flow_io.bin_recv (Flow_net.in_channel state.connection)
  >>= fun m ->
  of_result (Communication.Protocol.down_of_string
               ~mode:state.serialization_mode m)
    
let cmdf fmt = ksprintf Sequme_flow_sys.system_command fmt
  
type init_error =
[ `bin_recv of [ `exn of exn | `wrong_length of int * string ]
| `bin_send of [ `exn of exn | `message_too_long of string ]
| `io_exn of exn
| `no_password
| `system_command_error of
    string *
      [ `exited of int
      | `exn of exn
      | `signaled of int
      | `stopped of int ]
| `write_file_error of string * exn
| `message_serialization of
    Communication.Protocol.serialization_mode * string * exn
| `unexpected_message of Communication.Protocol.down
| `tls_context_exn of exn ]
with sexp_of
  
let connect ~host ~port =
  Sequme_flow_net.init_tls ();
  Sequme_flow_net.connect
    ~address:Unix.(ADDR_INET (Inet_addr.of_string_or_getbyname host, port))
    (`tls (`anonymous, `allow_self_signed))
  
let create_token () =
  Random.self_init ();
  String.init 128 (fun i -> Random.int 64 + 32 |! Char.of_int_exn)

let run_init_protocol ~state ~token_name ~host ~port ~configuration_file =
  Read_line.password ()
    ~prompt:(sprintf "Password for %s (like the one for the website): "
               state.user_name)
  >>= begin function
  | Some s -> return s
  | None -> error `no_password
  end
  >>= fun password ->
  let token = create_token () in
  send_message ~state (`new_token (state.user_name, password, token_name, token))
  >>= fun () ->
  recv_message ~state
  >>= begin function
  | `user_message _
  | `error `not_implemented as m -> error (`unexpected_message m)
  | `token_updated 
  | `token_created ->
    msg "The Authentication Token has been validated by the server \\o/"
    >>= fun () ->
    cmdf "mkdir -p %S" Filename.(dirname configuration_file)
    >>= fun () ->
    let config = Configuration.make ~host ~port ~auth_token:token in
    Sequme_flow_sys.write_file configuration_file
      ~content:(Configuration.to_string config)
    >>= fun () ->
    cmdf "chmod 400 %S" configuration_file
    >>= fun () ->
    msg "The configuration file as been successfully written, Happy Hacking!"
  | `error `wrong_authentication ->
    msg "The server has rejected your authentication; \
         the username or password must be wrong"
  | `error (`server_error s) ->
    msg "The server has run into trouble; you mya try again a bit later, \
         or if the problem persists call for help."
  end
  
let init_command =
  let open Command_line in
  let login = Unix.getlogin () in
  let default_token_name =
    let host = Unix.gethostname () in
    sprintf "%s@%s" login host in
  let default_config_file =
    let home = Sys.getenv "HOME" |! Option.value ~default:"/tmp/" in
    Filename.concat home ".config/gencore/client.conf" in
  basic ~summary:"Configure the application and get an authentication token"
    Spec.(
      Communication.Protocol.serialization_mode_flag ()
      ++ flag "-token-name"  (optional_with_default default_token_name string)
        ~doc:(sprintf 
                "<name> Give a name to the authentication token (default: %s)"
                default_token_name)
      ++ step (fun k configuration_file -> k ~configuration_file)
      ++ flag "-configuration-file" ~aliases:["c"]
        (optional_with_default default_config_file string)
        ~doc:(sprintf "<path> Use this configuration file (default: %s)"
                default_config_file)
      ++ flag "-user" ~aliases:["U"] (optional_with_default login string)
        ~doc:(sprintf "<name/email> Set the username (default: %s)" login)
      ++ anon ("HOST" %: string)
      ++ anon ("PORT" %: int)
    )
    (fun ~mode token_name ~configuration_file user_name host port ->
      run_flow ~on_error:(fun e ->
        eprintf "Client ends with Errors: %s"
          (Sexp.to_string_hum (sexp_of_init_error e)))
        begin
          connect ~host:"localhost" ~port:4002
          >>= fun connection ->
          let state = state connection ~mode ~user_name in
          run_init_protocol ~state ~token_name ~host ~port ~configuration_file
          >>= fun () ->
          Flow_net.shutdown connection
        end)
    
let () =
  Command_line.(
    run ~version:"0"
      (group ~summary:"Gencore's command-line application" [
        ("init", init_command);
      ]))
