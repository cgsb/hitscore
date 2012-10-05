

open Hitscore

open Core.Std
open Sequme_flow
open Sequme_flow_app_util
open Sequme_flow_list
open Sequme_flow_sys

let init ~log_file =
  Sequme_flow_net.init_tls ();
  Messages.message_head := "TEST>> ";
  return ()

let server_executable = ref "./server_main.native"
let client_executable = ref "./client_main.native"
    
let cmdf fmt =
  ksprintf (fun s ->
    msg "$ %s" s >>= fun () ->
    system_command s)
    fmt

let test_hello () =
  for_concurrent [`server; `client; `client; `killer ] begin function
  | `server ->
    cmdf "%s -cert cert.pem -key privkey-unsec.pem \
             -pid-file /tmp/test_hello.pid -profile dev 4002"
      !server_executable
  | `client  ->
    sleep 1. >>= fun () ->
    cmdf "%s info" !client_executable
  | `killer ->
    sleep 4. >>= fun () ->
    cmdf "kill `cat /tmp/test_hello.pid`"
    >>= fun () ->
    msg "Killed The Server!"
  end
  >>= fun _ ->
  return ()

type error = [
| `io_exn of exn
| `system_command_error of
    string *
      [ `exited of int
      | `exn of exn
      | `signaled of int
      | `stopped of int ]
] with sexp_of
  
let display_errors e =
  eprintf "TEST-ERROR:\n%s\n%!" (Sexp.to_string_hum (sexp_of_error e))
  

let () =
  Command_line.run_flow  ~on_error:(display_errors)
    (init ()
     >>= fun () ->
     test_hello ())


  
