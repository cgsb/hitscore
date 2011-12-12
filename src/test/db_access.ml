module Lwt_config = struct
  include Lwt
  include Lwt_chan
  let map_sequential l ~f = Lwt_list.map_s f l
  let log_error s = output_string Lwt_io.stderr s >>= fun () -> flush Lwt_io.stderr
end
module Hitscore_lwt = Hitscore.Make(Lwt_config)
module Hitscore_db = Hitscore_lwt.Layout
module PGOCaml = Hitscore_db.PGOCaml

open Hitscore_lwt.Result_IO

open Core.Std
let (|>) x f = f x
(* open Lwt 

let lwt_reraise = function
  | Ok o -> return o
  | Error e -> Lwt.fail e
*)

let notif s =
  let section = Lwt_log.Section.make "Notifications" in
  let logger = 
    Lwt_log.channel 
      ~template:"[$(date).$(milliseconds) $(name)($(pid)) \
                  $(section)/$(level)]: $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr () in
  bind_on_error
    (catch_io (Lwt_log.notice ~section ~logger) s)
    (fun e -> error (`lwt_log_exn e))

let result s =
  let section = Lwt_log.Section.make "Results" in
  let logger = 
    Lwt_log.channel 
      ~template:"[$(section)]: $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr () in
  bind_on_error
    (catch_io (Lwt_log.notice ~section ~logger) s)
    (fun e -> error (`lwt_log_exn e))
    
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
  print result "Adding %s (%s)" family_name email >>=
    (fun () ->
      Hitscore_db.Record_person.add_value ~dbh 
        ?print_name ~family_name ~email ?login ?note ?nickname:None)

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
  Hitscore_db.get_all_values ~dbh >>| List.length >>=
  fun vals ->
  Hitscore_db.get_all_evaluations ~dbh >>| List.length >>=
  fun evls ->
  Hitscore_db.File_system.get_all ~dbh >>| List.length >>=
  fun vols ->
  print result "Values: %d, Evaluations: %d, Volumes: %d\n" 
    vals evls vols

let ls_minus_r dbh =
  Hitscore_db.File_system.get_all ~dbh >>=
  of_list_sequential ~f:(fun vol -> 
    Hitscore_db.File_system.cache_volume ~dbh vol >>|
      (fun volume ->
        print result "Volume: %S\n" 
          Hitscore_db.File_system.(
            volume |> volume_entry_cache |> volume_entry |> entry_unix_path) >>=
        fun () ->
        match Hitscore_db.File_system.volume_trees volume with
        | Error (`cannot_recognize_file_type s) ->
          print notif "cannot_recognize_file_type %S??\n" s
        | Error (`inconsistency_inode_not_found i) ->
          print notif "inconsistency_inode_not_found %ld" i
        | Ok trees ->
          let paths = Hitscore_db.File_system.trees_to_unix_paths trees in
          of_list_sequential ~f:(print result "  |-> %s\n") paths >>=
          fun _ -> return ()))

let add_assemble_sample_sheet dbh =
  Hitscore_db.Record_flowcell.add_value 
    ~serial_name:(sprintf "MAC%dCXX" (Random.int 100))
    ~lanes:[| |]
    ~dbh >>=
  fun flowcell ->
    Hitscore_db.Function_assemble_sample_sheet.add_evaluation
      ~kind:`all_barcodes
      ~flowcell
      ~recomputable:true
      ~recompute_penalty:1.
      ~dbh >>=
  fun func ->
    print result "Added a function\n" >>=
  fun () ->
    return func

let start_all_inserted_assemblies dbh =
  Hitscore_db.Function_assemble_sample_sheet.get_all_inserted ~dbh >>= fun l ->
  print result "Got %d inserted assemble_sample_sheet's\n" 
    (List.length l) >>= fun () ->
  of_list_sequential l
    ~f:(Hitscore_db.Function_assemble_sample_sheet.set_started ~dbh)

let randomly_cancel_or_fail dbh =
  Hitscore_db.Function_assemble_sample_sheet.get_all_started ~dbh >>=
  fun l ->
  print result "Got %d started assemble_sample_sheet's\n" (List.length l) >>=
  fun () ->
  of_list_sequential l
    (fun f ->
      if Random.bool () then
        Hitscore_db.Function_assemble_sample_sheet.set_failed ~dbh f >>= fun _ ->
        print result "Set failed\n"
      else
        Hitscore_db.File_system.add_volume ~dbh
          ~kind:`sample_sheet_csv
          ~hr_tag:"AllBC"
          ~files:Hitscore_db.File_system.Tree.([file "SampleSheet.csv"]) >>=
        fun file ->
        Hitscore_db.Record_sample_sheet.add_value
          ~file ~note:"some note on the sample-sheet …" ~dbh >>=
        fun new_sample_sheet ->
          Hitscore_db.Function_assemble_sample_sheet.set_succeeded
            ~dbh ~result:new_sample_sheet f >>=
        fun _ ->
          print result "Added a sample-sheet and set succeeded\n")

let show_success dbh =
  let open Hitscore_db.Function_assemble_sample_sheet in
  get_all_succeeded ~dbh >>=
  of_list_sequential ~f:(fun success ->
    cache_evaluation ~dbh success >>=
    fun cache ->
    begin match get_arguments cache with
    | Error e ->
      print notif "get_arguments returned %s" (Exn.to_string e)
    | Ok {kind; flowcell} ->
      Hitscore_db.Record_flowcell.(
        (cache_value ~dbh flowcell) >>| get_fields >>=
            fun fields -> return fields.serial_name 
      ) >>=
      fun flowcell_name ->
      let kind_str =
        Hitscore_db.Enumeration_sample_sheet_kind.to_string kind in
      begin match get_result cache with
      | Ok assembled ->
        Hitscore_db.Record_sample_sheet.(
          cache_value ~dbh assembled >>| get_fields >>=
            fun f -> return f.note) >>=
        fun sample_sheet_note -> (* TODO: get also the file *)
        print result "Success found for:\n\
                  \    Flowcell: %s,\n    Kind: %s,\n    Note: %s\n" 
          flowcell_name kind_str  
          (Option.value ~default:"NONE" sample_sheet_note)
      | Error (`layout_inconsistency (`result_not_available)) ->
        print notif "get_result detected a DB inconsistency: \
                    result_not_available!\n"
      end
    end)


let test_sample_sheet_preparation ~dbh kind flowcell =
  let file = sprintf "/tmp/Sample_sheet_%s_%s.csv" flowcell
    (Hitscore_lwt.Layout.Enumeration_sample_sheet_kind.to_string kind) in
  print result "== Samplesheet:\n" >>= fun () ->
  Hitscore_lwt.Sample_sheet.preparation
    ~kind ~dbh flowcell >>= fun sample_sheet ->
  Lwt_io.(with_file ~mode:output file (fun chan ->
    Hitscore_lwt.Sample_sheet.output sample_sheet (wrap_io (fprint chan))))
  >>= fun () ->
  Hitscore_lwt.Sample_sheet.register_success ~dbh
    ~note:"Test the sample-sheet assembly" sample_sheet
  >>= fun (the_function, volpath, filespath) ->
  print result "Added the successful assembly of the sample-sheet" >>= fun () ->
  print result "Should add %s in %s/%s" file volpath (List.hd_exn filespath)



let test_lwt () =
  let hitscore_configuration = Hitscore_lwt.configure () in
  Hitscore_lwt.db_connect hitscore_configuration >>= 
  fun dbh ->
  notif "Starting" >>= fun _ ->
  count_all dbh >>= fun _ ->
  count_people dbh >>= fun _ ->
  add_random_guy dbh >>= fun _ ->
  add_random_guy dbh >>= fun _ ->
  add_random_guy dbh >>= fun _ ->
  count_people dbh >>= fun _ ->

  add_random_guy dbh >>=
  print_guy dbh >>= fun _ ->

  add_a_file dbh >>= fun _ ->
  add_a_file dbh >>= fun _ ->
  count_all dbh >>= fun _ ->
  add_assemble_sample_sheet dbh >>= fun _ ->
  add_assemble_sample_sheet dbh >>= fun _ ->
  add_assemble_sample_sheet dbh >>= fun _ ->
  add_assemble_sample_sheet dbh >>= fun _ ->
  count_all dbh >>= fun _ ->
  start_all_inserted_assemblies dbh >>= fun _ ->
  
  randomly_cancel_or_fail dbh >>= fun _ ->
  
  show_success dbh >>= fun _ -> 
  count_all dbh >>= fun _ ->
  ls_minus_r dbh >>= fun _ ->
  
  Hitscore_db.get_dump ~dbh >>=
    (fun dump ->
      let extract len s = try String.sub ~pos:0 ~len s with e -> s in 
      print result "DUMP (extract, 200 characters): \n%s...\n"
        (Hitscore_db.sexp_of_dump dump |> Sexplib.Sexp.to_string_hum
            |> (extract 200)) >>= fun _ ->
      let should_be_error = 
        of_list_sequential dump.Hitscore_db.function_assemble_sample_sheet 
          (Hitscore_db.Function_assemble_sample_sheet.insert_cache ~dbh)
      in
      double_bind should_be_error
        ~ok:(fun _ ->
          print notif "SHOULD NOT BE THERE !!!!")
        ~error:(function
          | `layout_inconsistency
              (`function_assemble_sample_sheet, 
               `insert_cache_did_not_return_one_id (table_name, i32l)) ->
            print notif "insert_cache detected a DB inconsistency: \
                      INSERT in %S did not return one id but all these: [%s]"
              table_name
              (String.concat ~sep:"; " (List.map i32l (sprintf "%ld")))
          | `pg_exn exn ->
            print result "Inserting an evaluation fails because the ID is \
                      already used:\n%s...\n" (Exn.to_string exn |> (extract 70))
          | (`layout_inconsistency (_, _)) | (`lwt_log_exn _) as lle ->
            error lle)
    ) >>= fun _ ->

  test_sample_sheet_preparation ~dbh `specific_barcodes "D0560ACXX" >>= fun () ->
  test_sample_sheet_preparation ~dbh `all_barcodes "D0560ACXX" >>= fun () ->
  test_sample_sheet_preparation ~dbh `specific_barcodes "D03M4ACXX" >>= fun () ->
  test_sample_sheet_preparation ~dbh `specific_barcodes "C01L9ACXX" >>= fun () ->

  print notif "Nice ending" >>= fun () ->
  notif "Closing the DB." >>= fun () ->
  Hitscore_lwt.db_disconnect hitscore_configuration dbh

let () =
  begin match Lwt_main.run (test_lwt ()) with
  | Error (`layout_inconsistency (m, e)) ->
    eprintf "\n=== BAD!! ====\nThe test ended with a layout_inconsistency!\n\n"
  | Error (`lwt_log_exn e) | Error (`pg_exn e) | Error (`io_exn e) ->
    eprintf "\n=== BAD!! ====\nThe test ended with an exn:\n%s\n"
      (Exn.to_string e)
  | Error (`barcode_not_found (b, p)) ->
    eprintf "\n=== BAD!! ====\nThe test ended with a barcoding error:\n\
      Barcode %ld of type %s was not found\n"
      b (Hitscore_lwt.Layout.Enumeration_barcode_provider.to_string p)
  | Ok () ->
    eprintf "Still good after Lwt_main.run\n"
  end


module P 
(*  : sig
    include Hitscore_config.IO_CONFIGURATION with type 'a t = 'a Lwt.t
  (* module type of Lwt_config *)
      
    val input_string : in_channel -> int -> string option Lwt.t
      
    module Process : sig
      type process
      val std
    end
    val with_process : ?env:(string list) -> string -> string list -> 
      f:(stdin:out_channel -> stdout:in_channel -> stderr:in_channel -> 'a t) ->
      'a t
  end *)
 = struct
  include Lwt_config

  let input_string i s =
    let b = String.make s 'A' in 
    input i b 0 s  >>= fun res ->
    if res > 0 then return (Some (String.sub b 0 res)) else return None  

  module Process = struct
    type t = Lwt_process.process_full
    let stdin t = t#stdin
    let stdout t = t#stdout
    let stderr t = t#stderr

  end

  let with_process ?env n al ~f =
    Lwt_process.with_process_full 
      ?env:(Option.map env Array.of_list)
      (n, Array.of_list al) (fun full -> f full)
end

let _ =
  let open P in
  Lwt_main.run 
    (Lwt.catch
       (fun () ->
         (with_process ~env:["TESTENV=sdfdsg"] "./ddd" ["echo" ; "bah"; "bouh"]
            ~f:(fun p ->
              let stdout = Process.stdout p in
              log_error "ddd ??\n" >>= fun () ->
              input_string stdout 400 >>= (function
                | Some s -> log_error "SOME: " >>= fun () -> log_error s 
                | None -> log_error "NONE") >>= fun () ->
              return ())))
       (fun e ->
         print log_error "error: %s\n" (Exn.to_string e)))
         
