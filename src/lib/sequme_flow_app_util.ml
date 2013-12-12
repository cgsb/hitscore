(** Utilities for applications around the flow monad. *)

open Core.Std
open Sequme_flow
open Sequme_flow_list
  

module Messages = struct

  let message_head = ref ">>>"
  let debug_head = ref "DEBUG:"

  let format_message head s =
    let indent_str = String.(make (length head + 1) ' ') in
    let l = String.split s ~on:'\n' in
    sprintf "%s %s\n"
      head 
      (List.map l (sprintf "%s") |! String.concat ~sep:("\n" ^ indent_str))


  let log_function  = ref (None : (string -> unit Lwt.t) option)

  let template = "$(message)"
  let init_log how =
    wrap_io (fun () ->
      let open Lwt in
      begin match how with
      | `file file_name ->
        Lwt_log.file ~mode:`Append ~template ~file_name ()
      | `channel channel ->
        return (Lwt_log.channel ~channel ~close_mode:`Keep ~template ())
      end
      >>= fun logger ->
      let lwt_log s = Lwt_log.notice ~logger s in
      log_function := Some lwt_log;
      return ())
      ()
    
  let log_string s =
    begin match !log_function with
    | None -> return ()
    | Some f ->
      wrap_io f (sprintf "%s %s" Time.(now () |! to_string)
                   (format_message ": " s))
    end
      
end
open Messages
  
let msg fmt =
  ksprintf (fun s ->
    wrap_io (Lwt_io.eprintf "%s%!") (format_message !message_head s)) fmt
    
let dbg fmt =
  ksprintf (fun s ->
    wrap_io (Lwt_io.eprintf "%s%!") (format_message !debug_head s)) fmt

let log fmt = ksprintf log_string fmt



module Command_line = struct
  include  Command

  let run_flow ~on_error m =
    match Lwt_main.run m with
    | Ok () -> ()
    | Error e -> on_error e


end

module Read_line = struct

  (*
  let password ?prompt () =
    wrap_io ()
      ~f:(fun () ->
        Lwt_preemptive.detach (fun () ->
          Core_extended.Readline.password ?prompt ()
        ) ()
      ) 
  *)



end
