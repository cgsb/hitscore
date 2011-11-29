module Lwt_thread = struct
  include Lwt
  include Lwt_chan
  let map_s = Lwt_list.map_s
  let err s = output_string Lwt_io.stderr s >> flush Lwt_io.stderr
end
module Hitscore_db = Hitscore_db_access.Make(Lwt_thread)
module PGOCaml = Hitscore_db.PGOCaml


open Core.Std
let (|>) x f = f x
open Lwt


let notif =
  let section = Lwt_log.Section.make "Notifications" in
  let logger = 
    Lwt_log.channel 
      ~template:"[$(date).$(milliseconds) $(name)($(pid)) \
                  $(section)/$(level)]: $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr () in
  Lwt_log.notice ~section ~logger
let result =
  let section = Lwt_log.Section.make "Results" in
  let logger = 
    Lwt_log.channel 
      ~template:"[$(section)]: $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr () in
  Lwt_log.notice ~section ~logger
    
let print = ksprintf

let count_people dbh = 
  Hitscore_db.Record_person.get_all ~dbh >>=
    (function
      | [] -> print result "Found no one"
      | l -> print result "Found %d persons" (List.length l))

let add_random_guy dbh =
  let print_name = if Random.bool () then Some "Print F. Name" else None in
  let family_name = sprintf "Family%d" (Random.int 4242) in
  let email = sprintf "pn%d@nyu.edu" (Random.int 9090) in
  let login = if Random.bool () then Some "login" else None in
  let note = if Random.bool () then Some "some note" else None in
  print result "Adding %s (%s)" family_name email >>
  Hitscore_db.Record_person.add_value ~dbh 
    ?print_name ~family_name ~email ?login ?note

let print_guy dbh guy =
  let open Hitscore_db.Record_person in
  cache_value guy dbh >>=
    (fun cache ->
      let {print_name; family_name; email; login; note } = get_fields cache in
      print result "%s %s <%s>" family_name
        (Option.value_map ~default:"" ~f:(sprintf "(%s)") print_name)
        email)


let add_a_file dbh =
  Hitscore_db.File_system.add_file ~dbh
    ~kind:`bioanalyzer_directory
    ~hr_tag:"LdfdjkR20111129"
    ~files:Hitscore_db.File_system.Path.(
      [ file "foo.pdf"; file "foo.xad"; 
        dir "otherstuff" [ file ~t:`blob "README"; 
                           file "data.db";
                           opaque "opaquedir"] ])

let count_all dbh =
  lwt vals = Hitscore_db.get_all_values ~dbh >|= List.length in
  lwt evls = Hitscore_db.get_all_evaluations ~dbh >|= List.length in
  print result "Values: %d, Evaluations: %d\n" vals evls

let test_lwt =
  lwt dbh = PGOCaml.connect () in
  try_lwt 
    notif "Starting" >>
    count_all dbh >>
    count_people dbh >>
    add_random_guy dbh >>
    add_random_guy dbh >>
    add_random_guy dbh >>
    count_people dbh >>

    add_random_guy dbh >>=
    print_guy dbh >>

    add_a_file dbh >>
    add_a_file dbh >>
    count_all dbh >>

    print notif "Nice ending" 
  finally
    notif "Closing the DB." >>
    PGOCaml.close dbh 


let () =
  match state test_lwt with
  | Return _ -> eprintf "returns\n"
  | Fail _ -> eprintf "fails\n"
  | Sleep ->
    eprintf "Still sleeping …\n"; 
    Lwt_main.run test_lwt

