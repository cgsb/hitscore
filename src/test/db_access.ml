module Lwt_config = struct
  include Lwt
  include Lwt_chan
  let map_sequential l ~f = Lwt_list.map_s f l
  let log_error s = output_string Lwt_io.stderr s >> flush Lwt_io.stderr
end
module Hitscore_lwt = Hitscore.Make(Lwt_config)
module Hitscore_db = Hitscore_lwt.Layout
module PGOCaml = Hitscore_db.PGOCaml


open Core.Std
let (|>) x f = f x
open Lwt

let lwt_reraise = function
  | Ok o -> return o
  | Error e -> Lwt.fail e

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
    ?print_name ~family_name ~email ?login ?note ?nickname:None

let print_guy dbh guy =
  let open Hitscore_db.Record_person in
  cache_value guy dbh >>=
    (fun cache ->
      let {print_name; family_name; email; login; note } = get_fields cache in
      print result "%s %s %s"
        (Option.value ~default:"" family_name)
        (Option.value_map ~default:"" ~f:(sprintf "(%s)") print_name)
        (Option.value_map ~default:"NoEmail" ~f:(sprintf "<%s>") email))


let add_a_file dbh =
  Hitscore_db.File_system.add_volume ~dbh
    ~kind:`bioanalyzer_directory
    ~hr_tag:"LdfdjkR20111129"
    ~files:Hitscore_db.File_system.Tree.(
      [ file "foo.pdf"; file "foo.xad"; 
        dir "otherstuff" [ file ~t:`blob "README"; 
                           file "data.db";
                           opaque "opaquedir"] ])

let count_all dbh =
  lwt vals = Hitscore_db.get_all_values ~dbh >|= List.length in
  lwt evls = Hitscore_db.get_all_evaluations ~dbh >|= List.length in
  lwt vols = Hitscore_db.File_system.get_all ~dbh >|= List.length in
  print result "Values: %d, Evaluations: %d, Volumes: %d\n" vals evls vols

let ls_minus_r dbh =
  lwt vols = Hitscore_db.File_system.get_all ~dbh  in
  Lwt_list.map_s (fun vol -> 
    Hitscore_db.File_system.cache_volume ~dbh vol >|=
      (fun volume ->
        print result "Volume: %S\n" 
          Hitscore_db.File_system.(
            volume |> volume_entry_cache |> volume_entry |> entry_unix_path) >>
        let trees = Hitscore_db.File_system.volume_trees volume in
        let paths = Hitscore_db.File_system.trees_to_unix_paths trees in
        Lwt_list.map_s (print result "  |-> %s\n") paths)) vols

let add_assemble_sample_sheet dbh =
  lwt flowcell = 
    Hitscore_db.Record_flowcell.add_value 
      ~serial_name:(sprintf "MAC%dCXX" (Random.int 100))
      ~lanes:[| |]
      ~dbh in
  lwt func =
    Hitscore_db.Function_assemble_sample_sheet.add_evaluation
      ~kind:`all_barcodes
      ~flowcell
      ~recomputable:true
      ~recompute_penalty:1.
      ~dbh in
  print result "Added a function\n" >>
  return func

let start_all_inserted_assemblies dbh =
  Hitscore_db.Function_assemble_sample_sheet.get_all_inserted ~dbh >>=
  (fun l ->
    print result "Got %d inserted assemble_sample_sheet's\n" (List.length l) >>
    Lwt_list.map_s 
      (Hitscore_db.Function_assemble_sample_sheet.set_started ~dbh) l)

let randomly_cancel_or_fail dbh =
  Hitscore_db.Function_assemble_sample_sheet.get_all_started ~dbh >>=
  (fun l ->
    print result "Got %d started assemble_sample_sheet's\n" (List.length l) >>
    Lwt_list.map_s 
      (fun f ->
        if Random.bool () then
          Hitscore_db.Function_assemble_sample_sheet.set_failed ~dbh f >>
          print result "Set failed\n"
        else
          Hitscore_db.File_system.add_volume ~dbh
            ~kind:`sample_sheet_csv
            ~hr_tag:"AllBC"
            ~files:Hitscore_db.File_system.Tree.([file "SampleSheet.csv"]) >>=
          (fun file ->
            Hitscore_db.Record_sample_sheet.add_value
              ~file ~note:"some note on the sample-sheet …" ~dbh >>=
            (fun result ->
              Hitscore_db.Function_assemble_sample_sheet.set_succeeded
                ~dbh ~result f) >>
              print result "Added a sample-sheet and set succeeded\n")) l)

let show_success dbh =
  let open Hitscore_db.Function_assemble_sample_sheet in
  get_all_succeeded ~dbh >>=
  Lwt_list.map_s (fun success ->
    cache_evaluation ~dbh success >>=
    (fun cache ->
      let {kind; flowcell} = get_arguments cache in
      lwt flowcell_name =
        Hitscore_db.Record_flowcell.(
          lwt {serial_name; _ } = (cache_value ~dbh flowcell) >|= get_fields in
          return serial_name ) in
      let kind_str = Hitscore_db.Enumeration_sample_sheet_kind.to_string kind in
      lwt sample_sheet_note = (* TODO get also the file *)
        let result = get_result cache in
        Hitscore_db.Record_sample_sheet.(
          lwt {note; _} = cache_value ~dbh result >|= get_fields in
          return note) in
      print result "Success found for:\n\
                    \    Flowcell: %s,\n    Kind: %s,\n    Note: %s\n" 
        flowcell_name kind_str 
        (Option.value ~default:"NONE" sample_sheet_note) ;))

let test_lwt =
  let hitscore_configuration = Hitscore_lwt.configure () in
  lwt dbh = Hitscore_lwt.db_connect hitscore_configuration >>= lwt_reraise in
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
    add_assemble_sample_sheet dbh >>
    add_assemble_sample_sheet dbh >>
    add_assemble_sample_sheet dbh >>
    add_assemble_sample_sheet dbh >>
    count_all dbh >>
    start_all_inserted_assemblies dbh >>

    randomly_cancel_or_fail dbh >>

    show_success dbh >> 
    count_all dbh >>
    ls_minus_r dbh >>

    Hitscore_db.get_dump ~dbh >>=
    (fun dump ->
      let extract s = try String.sub ~pos:0 ~len:200 s with e -> s in 
      print result "DUMP (extract, 200 characters): \n%s...\n"
        (Hitscore_db.sexp_of_dump dump |> Sexplib.Sexp.to_string_hum |> extract) >>
      try_lwt (* This should fail: *)
        Lwt_list.map_s
          (Hitscore_db.Function_assemble_sample_sheet.insert_cached ~dbh)
          dump.Hitscore_db.function_assemble_sample_sheet >>
        print notif "SHOULD NOT BE THERE !!!!"
      with
      | exn ->
        print result "Inserting an evaluation fails because the ID is \
                      already used:\n%s\n" (Exn.to_string exn);
    ) >>

    print notif "Nice ending" 
  finally
    notif "Closing the DB." >>
    Hitscore_lwt.db_disconnect hitscore_configuration dbh >>= lwt_reraise


let () =
  match state test_lwt with
  | Return _ -> eprintf "returns\n"
  | Fail _ -> eprintf "fails\n"
  | Sleep ->
    eprintf "Still sleeping …\n%!"; 
    Lwt_main.run test_lwt

