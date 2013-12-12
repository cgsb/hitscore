open Core.Std
open Sequme_flow
  
module Timeout = struct
  type t = [
  | `global_default
  | `seconds of float
  | `none
  ]

  let _global_default = ref 60.
  let set_global_default v = _global_default := v
  let get_global_default () = !_global_default

  let do_io timeout ~f =
    match timeout with
    | `none -> f ()
    | some ->
      let time =
        match some with
        | `none -> assert false
        | `global_default -> !_global_default
        | `seconds f -> f in
      Lwt.catch
        begin fun () ->
          Lwt_unix.with_timeout time f
        end
        begin function
        | Lwt_unix.Timeout -> error (`timeout time)
        | e -> error (`io_exn e)
        end

end 

let  write_file file ~content =
  catch_io () ~f:Lwt_io.(fun () ->
    with_file ~mode:output file (fun i -> write i content))
  |! bind_on_error ~f:(fun e -> error (`write_file_error (file, e)))

let read_file ?(timeout=`global_default) file =
  Timeout.do_io timeout ~f:(fun () ->
    catch_io () ~f:Lwt_io.(fun () ->
      with_file ~mode:input file (fun i -> read i))
    |! bind_on_error ~f:(fun e -> error (`read_file_error (file, e))))
  >>< begin function
  | Ok o -> return o
  | Error (`io_exn e) -> error (`read_file_error (file, e))
  | Error (`read_file_error (file, e)) -> error (`read_file_error (file, e))
  | Error (`timeout time) -> error (`read_file_timeout (file, time))
  end
        
let discriminate_process_status s ret =
  begin match ret with
  | Lwt_unix.WEXITED 0 -> return ()
  | Lwt_unix.WEXITED n -> error (`system_command_error (s, `exited n))
  | Lwt_unix.WSIGNALED n -> error (`system_command_error (s, `signaled n))
  | Lwt_unix.WSTOPPED n -> error (`system_command_error (s, `stopped n))
  end
      
let system_command s =
  bind_on_error ~f:(fun e -> error (`system_command_error (s, `exn e)))
    (catch_io () ~f:Lwt_io.(fun () -> Lwt_unix.system s))
  >>= fun ret ->
  discriminate_process_status s ret

let sleep f =
  wrap_io Lwt_unix.sleep f

    
let get_system_command_output s =
  bind_on_error ~f:(fun e -> error (`system_command_error (s, `exn e)))
    (catch_io
       Lwt.(fun () ->
         let inprocess = Lwt_process.(open_process_full (shell s)) in
         Lwt_list.map_p Lwt_io.read
           [inprocess#stdout; inprocess#stderr; ]
         >>= fun output ->
         inprocess#status >>= fun status ->
         return (status, output))
       ())
  >>= fun (ret, output) ->
  discriminate_process_status s ret
  >>= fun () ->
  begin match output with
  | [out; err] -> return (out, err)
  | _ -> assert false
  end

