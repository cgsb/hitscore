open Core.Std

module Lwt_thread = struct
  include Lwt
  include Lwt_chan
end
module PGOCaml = PGOCaml_generic.Make(Lwt_thread)
open Lwt

module Hitscore_db = Hitscore_db_access.Make(PGOCaml)


let note =
  let section = Lwt_log.Section.make "Notifications" in
  let logger = 
    Lwt_log.channel 
      ~template:"[$(date).$(milliseconds) $(name)($(pid)) \
                  $(section)/$(level)]: $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr () in
  Lwt_log.notice ~section ~logger
    
let print = ksprintf


let test_lwt =
  lwt dbh = PGOCaml.connect () in
  try_lwt 
    note "Starting" >>

    Hitscore_db.Record_person.get_all dbh >>=
    (function
      | [] -> print note "Found nothing"
      | l -> print note "Found %d elements" (List.length l)) >>
    
    print note "Nice ending" 
  finally
    note "Closing the DB." >>
    PGOCaml.close dbh 


let () =
  match state test_lwt with
  | Return _ -> eprintf "returns\n"
  | Fail _ -> eprintf "fails\n"
  | Sleep ->
    eprintf "Still sleeping …\n"; 
    Lwt_main.run test_lwt

