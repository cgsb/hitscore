#! /usr/bin/env ocaml

#use "topfind"
#thread
#require "hitscore"

open Core.Std


module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)

open Hitscore_threaded
open Result_IO


let fill profile csv_file =
  
  let config_file =
    sprintf "%s/.config/hitscore/config.sexp"
      (Option.value_exn_message "This environment has no $HOME !"
         (Sys.getenv "HOME")) in
  let config = In_channel.(with_file config_file ~f:input_all) in
  let configuration =
    Hitscore_threaded.Configuration.(
      parse_str config
      >>= fun c ->
      use_profile c profile)
    |! function
      | Ok o -> o
      | Error (`configuration_parsing_error e) ->
        eprintf "Error while parsing configuration: %s\n" (Exn.to_string e);
        failwith "STOP"
      | Error (`profile_not_found s) ->
        eprintf "Profile %S not found in config-file\n" s;
        failwith "STOP"
  in      
  let loaded = Csv.load ~separator:',' csv_file in
  let sanitized =
    let sanitize s =
      (String.split_on_chars ~on:[' '; '\t'; '\n'; '\r'] s)
      |! List.filter ~f:((<>) "") |! String.concat ~sep:" "
    in
    List.map loaded ~f:(List.map ~f:sanitize)
  in
  let mem = ref [] in
  List.iter sanitized ~f:(function
  | date :: a_or_b :: fcid :: _ when a_or_b = "A" || a_or_b = "B" ->
    mem := (date, (a_or_b, fcid)) :: !mem
  | _ -> ());
  let dates = List.map !mem fst |! List.dedup in
  let module HSR = Layout.Record_hiseq_run in
  let module FC = Layout.Record_flowcell in
  with_database ~configuration ~f:(fun ~dbh ->
    List.iter dates ~f:(fun d ->
      let anb =
        List.find_all !mem ~f:(fun a -> fst a = d) |! List.sort ~cmp:compare in
      printf "%s: " d;
      List.iter anb (fun (d, (ab, fcid)) -> printf "%s:%s " ab fcid);
      let afcid =
        List.filter_map anb (function
        | (d, ("A", fcid)) -> Some fcid | _ -> None)
        |! List.hd in
      let bfcid = 
        List.filter_map anb (function
        | (d, ("B", fcid)) -> Some fcid | _ -> None)
        |! List.hd in
      let fc o =
        Option.bind o (fun id ->
          Layout.Search.record_flowcell_by_serial_name ~dbh id
          |! Result.ok_exn ~fail:(Failure "search flowcell wrong")
          |! List.hd )
      in
      let afc = fc afcid and bfc = fc bfcid in
      Option.iter afc (fun d ->printf "A:%ld " d.FC.id);
      Option.iter bfc (fun d ->printf "B:%ld " d.FC.id);
      printf "\n";
      if afc <> None || bfc <> None then (
        HSR.add_value ~dbh ~date:(Time.of_string (d ^ " 10:00"))
          ?flowcell_a:afc ?flowcell_b:bfc ?note:None
        |! Result.ok_exn ~fail:(Failure "add hsr wrong") |! Pervasives.ignore
      );

    );
    return ()
  ) |! Result.ok_exn ~fail:(Failure "db connection");
  ()
    
let () =
  let args = Array.to_list Sys.argv in
  match args with
  | exec :: profile :: csv :: [] -> fill profile csv
  | _ ->
    printf "usage: %s <profile> <csv-run-plan>\n" Sys.argv.(0)
      
