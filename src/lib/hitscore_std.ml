include Core.Std


include Sequme_flow
include Sequme_flow_list
include Sequme_flow_sys
  

module Lwt_config = struct
  include Lwt
  include Lwt_chan
end
module PG = PGOCaml_generic.Make(Lwt_config)
(* let (|>) x f = f x *)

let canonical_path s =
  match Filename.parts s |! List.reduce ~f:Filename.concat with
  | Some s -> s
  | None -> failwithf "FATAL : (canonical_path %S) returned None!" s ()

let dbg fmt = ksprintf (fun s -> eprintf "HS-DBG: %s\n%!" s) fmt

let dbgt fmt =
  ksprintf (fun s -> dbg "%.4f: %s" Time.(now ()|> to_float) s) fmt

let flow_detach f =
  Lwt_preemptive.detach (fun () -> Ok (f ())) ()
let flow_yield () =
  wrap_io (fun () -> Lwt_main.yield ()) ()

let flow_invoke ~on_exn (f : unit -> 'b) =
  let open Lwt in
  let lwt_input, unix_output = Lwt_unix.pipe_in () in
  Lwt_io.flush_all ()
  >>= fun () ->
  begin match Lwt_unix.fork() with
  | -1 -> 
    dbgt "PROBLEM !!! Fork FAILED !!!";
    return (Ok (f ()))
  | 0 ->
    dbgt "Subprocess %d STARTED" (Caml.Unix.getpid ());
    let output = Unix.out_channel_of_descr unix_output in
    Marshal.(to_channel output (try f () with e -> on_exn e) [Closures]);
    Out_channel.close output;
    dbgt "Subprocess %d ENDS" (Caml.Unix.getpid ());
    exit 0
  | pid ->
    let channel = Lwt_io.(of_fd ~mode:input lwt_input) in
    Lwt_io.read channel
    >>= fun data ->
    let v = Marshal.from_string data (String.length data) in
    Lwt_unix.waitpid [] pid
    >>= fun (_, _) ->
    Lwt_io.close channel
    >>= fun () ->
    return (Ok v)
  end
  
