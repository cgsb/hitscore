open Core.Std
  

module System = struct

  let command_exn s = 
    let status = Unix.system s in
    if not (Unix.Process_status.is_ok status) then
      ksprintf failwith "System.command_exn: %s" 
        (Unix.Process_status.to_string_hum status)
    else
      ()

  let command s =
    let status = Unix.system s in
    if (Unix.Process_status.is_ok status) then
      Ok ()
    else
      Error (`sys_error (`command (s, status)))

  let command_to_string s =
    Unix.open_process_in s |! In_channel.input_all
        
end
module XML = struct
  include Xmlm
  let in_tree i = 
    let el tag childs = `E (tag, childs)  in
    let data d = `D d in
    input_doc_tree ~el ~data i
end
