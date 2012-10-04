

open Hitscore
open Core.Std
open Sequme_flow

let main () =
  return ()

let () =
  match Lwt_main.run (main ()) with
  | Ok () -> ()
  | Error e ->
    eprintf "END WITH ERROR:\n%!"
