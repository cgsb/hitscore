
open Core.Std

let () =
  match Array.to_list Sys.argv with
  | exec :: "-h" :: _
  | exec :: "-help" :: _
  | exec :: "--help" :: _
  | exec :: "help" :: _ ->
    printf "usage: %s <cmd> [OPTIONS | ARGS]\n" exec
  | _ ->
    eprintf "usage: hitscore <cmd> [OPTIONS | ARGS]\n" 

