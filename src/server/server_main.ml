

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

let init ~configuration_file ~profile ~log_file =
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

type error = 
[ `configuration_parsing_error of exn
| `io_exn of exn
| `profile_not_found of string
| `read_file_error of string * exn ]
with sexp_of

let string_of_error e =
  Sexp.to_string_hum (sexp_of_error e)
  
    
let command =
  let open Command_line in
  basic ~summary:"Hitscore's server application"
    Spec.(
      with_profile ()
      ++ step (fun k log_file -> k ~log_file)
      ++ flag "log-file" ~aliases:["-L"] (optional_with_default "-" string)
        ~doc:"<path> Log to file <path> (or “-” for stdout; the default)"
      ++ anon (sequence "ARGS" string)
    )
    (fun ~profile ~configuration_file ~log_file args ->
      run_flow ~on_error:(fun e ->
        eprintf "End with ERRORS: %s" (string_of_error e))
        begin
          init ~profile ~configuration_file ~log_file
          >>= fun configuration ->
          dbg "Command not implemented!"
        end)

    
let () =
  Command_line.(run ~version:"0" command)
    

