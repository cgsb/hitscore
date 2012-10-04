

open Hitscore
open Core.Std
open Sequme_flow
open Sequme_flow_app_util


    

let main () =
  dbg "Starting: %s" Time.(now () |! to_string) >>= fun () ->
  return ()


let info_command =
  let open Command_line in
  basic ~summary:"Get information"
    Spec.(
      anon (sequence "ARGS" string)
    )
    (fun args ->
      run_flow ~on_error:(function
      | `io_exn e ->
        eprintf "End with ERRORS: %s" Exn.(to_string e))
        begin
          dbg "command not implemented"
        end)

    
let () =
  Command_line.(
    run ~version:"0"
      (group ~summary:"Gencore's command-line application" [
        ("info", info_command);
      ]))
