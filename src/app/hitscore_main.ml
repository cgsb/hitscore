
open Core.Std
let (|>) x f = f x


open Hitscore_app_util
open Hitscore_threaded


module Configuration_file = struct

  let default () =
    sprintf "%s/.config/hitscore/config.sexp"
      (Option.value_exn_message "This environment has no $HOME !"
         (Sys.getenv "HOME"))

  let iter = List.iter

  let print_config config =
    let open Option in
    let open Hitscore_threaded.Configuration in
    iter (root_path config) (printf "Root path: %S\n");
    iter (vol_path config) (printf "  VFS-Volumes path: %S\n");
    printf "  Root-dir writers: [%s]\n"
      (String.concat ~sep:", " (root_writers config));
    iter (root_group config) (printf "  Roor-dir group: %S\n");
    iter (raw_data_path config) (printf "Raw-data path: %S\n");
    iter (hiseq_data_path config) (printf "  Hiseq raw-data: %S\n");
    iter (work_path config) (printf "Work directory: %S\n");
    iter (db_host     config) (printf "DB host     : %S\n"); 
    iter (db_port     config) (printf "   port     : %d\n"); 
    iter (db_database config) (printf "   database : %S\n"); 
    iter (db_username config) (printf "   username : %S\n"); 
    iter (db_password config) (printf "   password : %S\n");
    ()

  let print_env () =
    let open Option in
    let open Hitscore_threaded in
    iter (Sys.getenv "PGHOST") (printf "Env: PGHOST : %S\n");
    iter (Sys.getenv "PGPORT") (printf "Env: PGPORT : %S\n");
    iter (Sys.getenv "PGUSER") (printf "Env: PGUSER : %S\n");
    iter (Sys.getenv "PGDATABASE") (printf "Env: PGDATABASE : %S\n");
    iter (Sys.getenv "PGPASSWORD") (printf "Env: PGPASSWORD : %S\n");
    ()
    
  let print_all profiles =
    let open Hitscore_threaded.Configuration in
    List.iter (profile_names profiles) (fun n ->
      printf "** Configuration %S:\n" n;
      print_config (use_profile profiles n |! Result.ok_exn ~fail:Not_found));
    printf "** Environment:\n";
    print_env ();
    ()

  let export_env config command =
    let open Option in
    let open Hitscore_threaded.Configuration in
    let cmd = ref "" in
    let f s = cmd := !cmd ^ s in
    let print = ksprintf in
    print f "unset PGHOST ";
    print f "PGPORT ";
    print f "PGDATABASE ";
    print f "PGUSER  ";
    print f "PGPASSWORD\n";

    iter (db_port config)       (print f "export PGPORT=%d ; \n"    ); 
    iter (db_host     config)   (print f "export PGHOST=%s ; \n"    ); 
    iter (db_database config)   (print f "export PGDATABASE=%s ; \n"); 
    iter (db_username config)   (print f "export PGUSER=%s ; \n"    ); 
    iter (db_password config)   (print f "export PGPASSWORD=%s ; \n");
    printf "Running %S with:\n%s\n\
            ========================================\
            ========================================\n%!" command !cmd;
    System.command_exn (!cmd ^ " " ^ command);
    ()

end




module All_barcodes_sample_sheet = struct


  let illumina_barcodes = [
    1, "ATCACG";
    2, "CGATGT";
    3, "TTAGGC";
    4, "TGACCA";
    5, "ACAGTG";
    6, "GCCAAT";
    7, "CAGATC";
    8, "ACTTGA";
    9, "GATCAG";
    10, "TAGCTT";
    11, "GGCTAC";
    12, "CTTGTA"
  ]

  let bioo_barcodes = [
    1 ,"CGATGT";
    2 ,"TGACCA";
    3 ,"ACAGTG";
    4 ,"GCCAAT";
    5 ,"CAGATC";
    6 ,"CTTGTA";
    7 ,"ATCACG";
    8 ,"TTAGGC";
    9 ,"ACTTGA";
    10,"GATCAG";
    11,"TAGCTT";
    12,"GGCTAC";
    13,"AGTCAA";
    14,"AGTTCC";
    15,"ATGTCA";
    16,"CCGTCC";
    17,"GTAGAG";
    18,"GTCCGC";
    19,"GTGAAA";
    20,"GTGGCC";
    21,"GTTTCG";
    22,"CGTACG";
    23,"GAGTGG";
    24,"GGTAGC";
    25,"ACTGAT";
    26,"ATGAGC";
    27,"ATTCCT";
    28,"CAAAAG";
    29,"CAACTA";
    30,"CACCGG";
    31,"CACGAT";
    32,"CACTCA";
    33,"CAGGCG";
    34,"CATGGC";
    35,"CATTTT";
    36,"CCAACA";
    37,"CGGAAT";
    38,"CTAGCT";
    39,"CTATAC";
    40,"CTCAGA";
    41,"GCGCTA";
    42,"TAATCG";
    43,"TACAGC";
    44,"TATAAT";
    45,"TCATTC";
    46,"TCCCGA";
    47,"TCGAAG";
    48,"TCGGCA";
  ]

  let make ~flowcell ~specification output_string = 
    let config =
      List.map specification
        ~f:(function
          | "I1" -> 1, "I", illumina_barcodes 
          | "I2" -> 2, "I", illumina_barcodes 
          | "I3" -> 3, "I", illumina_barcodes 
          | "I4" -> 4, "I", illumina_barcodes 
          | "I5" -> 5, "I", illumina_barcodes 
          | "I6" -> 6, "I", illumina_barcodes 
          | "I7" -> 7, "I", illumina_barcodes 
          | "I8" -> 8, "I", illumina_barcodes 
          | "B1" -> 1, "B", bioo_barcodes 
          | "B2" -> 2, "B", bioo_barcodes 
          | "B3" -> 3, "B", bioo_barcodes 
          | "B4" -> 4, "B", bioo_barcodes 
          | "B5" -> 5, "B", bioo_barcodes 
          | "B6" -> 6, "B", bioo_barcodes 
          | "B7" -> 7, "B", bioo_barcodes 
          | "B8" -> 8, "B", bioo_barcodes 
          | "N1" -> 1, "N", [ 0, "" ] 
          | "N2" -> 2, "N", [ 0, "" ] 
          | "N3" -> 3, "N", [ 0, "" ] 
          | "N4" -> 4, "N", [ 0, "" ] 
          | "N5" -> 5, "N", [ 0, "" ] 
          | "N6" -> 6, "N", [ 0, "" ] 
          | "N7" -> 7, "N", [ 0, "" ] 
          | "N8" -> 8, "N", [ 0, "" ] 
          | s ->
            failwith (sprintf  "Can't understand %S" s)
        ) in
    let head = 
      "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,\
         Operator,SampleProject\n" in
    ksprintf output_string "%s" head;
    List.iter config ~f:(fun (lane, letter, barcodes) ->
      List.iter barcodes ~f:(fun (id, barcode) ->
        ksprintf output_string "%s,%d,%s%02d%s,,%s,,N,,,Lane%d\n"
          flowcell lane letter id barcode barcode lane;
      );
    );
    ()


end

module PBS_script_generator = struct

  let make_script
      ~root ~email ?queue ?(wall_hours=12) ?(nodes=0) ?(ppn=0) 
      ?(variables=[]) ?template name =
    let out_dir_name = sprintf "%s/%s" root name in
    System.command_exn (sprintf "mkdir -p %s" out_dir_name); 
    let script, script_contents =
      let buf = Buffer.create 42 in
      (Buffer.add_string buf, fun () -> Buffer.contents buf) in
    let pr = ksprintf in
    pr script "#!/bin/bash\n\n";
    pr script "#PBS -m abe\n";
    pr script "#PBS -M %s\n" email;
    pr script "#PBS -l %swalltime=%d:00:00\n"
      (match nodes, ppn with
      | 0, 0 -> ""
      | n, m -> sprintf "nodes=%d:ppn=%d," n m)
      wall_hours;
    pr script "#PBS -V\n#PBS -o %s/%s.stdout\n#PBS -e %s/%s.stderr\n"
      out_dir_name name out_dir_name name;
    pr script "#PBS -N %s\n" name;
    Option.iter queue (fun s -> pr script "#PBS -q %s\n" s);
    pr script "export NAME=%s\n" name;
    pr script "export OUT_DIR=%s/\n\n" out_dir_name;
    pr script "%s\n\n"
      (String.concat ~sep:"\n" (List.map ~f:(sprintf "export %s") variables));
    pr script "echo \"Script $NAME Starts on `date -R`\"\n\n";

    Option.iter template (fun s ->
      pr script "# User script:\n%s" s;);

    pr script "echo \"Script $NAME Ends on `date -R`\"\n\n";

    Out_channel.with_file (sprintf "%s/script_%s.pbs" out_dir_name name) 
      ~f:(fun o -> fprintf o "%s" (script_contents ()));
    ()
    


  let parse_cmdline usage_prefix next_arg =
    Arg.current := next_arg;
    let email = 
      ref (sprintf "%s@nyu.edu" 
             (Option.value ~default:"NOTSET" (Sys.getenv "LOGNAME"))) in
    let queue = 
      let groups =
        System.command_to_string "groups" |> 
            String.split_on_chars ~on:[ ' '; '\t'; '\n' ] in
      match List.find groups ((=) "cgsb") with
      | Some _ -> ref (Some "cgsb-s")
      | None -> ref None in
    let template = ref None in
    let variables = ref [] in
    let root = ref (Option.value ~default:"NOTSET" (Sys.getenv "PWD")) in
    let nodes = ref 0 in
    let ppn = ref 0 in
    let options = [
      ( "-email", 
        Arg.Set_string email,
        sprintf "<address>\n\tSet the email (default, inferred: %s)." !email);
      ( "-queue", 
        Arg.String (fun s -> queue := Some s),
        sprintf "<name>\n\tSet the queue (default, inferred: %s)." 
          (Option.value ~default:"None"  !queue));
      ( "-var", 
        Arg.String (fun s -> variables := s :: !variables),
        "<NAME=val>\n\tAdd an environment variable.");
      ( "-template", 
        Arg.String (fun s -> template := Some s),
        "<path>\n\tGive a template file.");
      ( "-root", 
        Arg.Set_string root,
        sprintf "<path>\n\tSet the root directory (default: %s)." !root);
      ( "-nodes-ppn", 
        Arg.Tuple [ Arg.Set_int nodes; Arg.Set_int ppn],
        "<n> <m>\n\tSet the number of nodes and processes per node.");
    ] in
    let names = ref [] in
    let anon s = names := s :: !names in
    let usage = sprintf "%s [OPTIONS] <scriptnames>" usage_prefix in
    Arg.parse options anon usage;
    let template =
      Option.map !template (fun f -> In_channel.(with_file f ~f:input_all)) in
    List.iter !names
      (fun name ->
        make_script name ~root:!root ~nodes:!nodes ~ppn:!ppn ?template
          ~variables:!variables ?queue:!queue ~email:!email);
    ()




end


module Gen_BclToFastq = struct


  let casava_182_template unaligned = 
    sprintf " 
. /share/apps/casava/1.8.2/intel/env.sh

cd %s

make -j8 \
  1> $OUT_DIR/make.stdout \
  2> $OUT_DIR/make.stderr
" unaligned

  let prepare ?(mismatches=[0;1]) name basecalls sample_sheet =
    let root = (Option.value ~default:"NOTSET" (Sys.getenv "PWD")) in
    let script, script_contents =
      let buf = Buffer.create 42 in
      (Buffer.add_string buf, fun () -> Buffer.contents buf) in
    let cmd os fmt = 
      ksprintf (fun s -> os (sprintf "echo %S ; \n%s\n\
            if [ $? -ne 0 ]; then exit 1 ; fi\n" s s)) fmt in
    let conf_b2f mm =
      let unaligned = sprintf "%s/%s_M%d/Unaligned" root name mm in
      cmd script "mkdir %s/%s_M%d" root name mm;
      cmd script "configureBclToFastq.pl --fastq-cluster-count 800000000 \
                  --input-dir %s \
                  --output-dir %s \
                  --sample-sheet %s \
                  --mismatches %d"
        basecalls unaligned sample_sheet mm;
      PBS_script_generator.make_script  
        ~root
        ~email:(sprintf "%s@nyu.edu" 
                  (Option.value ~default:"NOTSET" (Sys.getenv "LOGNAME"))) 
        ~queue:"cgsb-s"
        ~wall_hours:12
        ~nodes:1 ~ppn:8 ~template:(casava_182_template unaligned) 
        (sprintf "PBSRuntime_%s_M%d" name mm);
    in
    cmd script ". /share/apps/casava/1.8.2/intel/env.sh\n\n";
    List.iter mismatches conf_b2f;
    let tmp = (Filename.temp_file "bcl2fast_preparation" ".sh") in
    Out_channel.(with_file tmp
                   ~f:(fun o -> output_string o (script_contents ())));
    System.command_exn (sprintf "sh %s" tmp);
    ()

end

module Dumps = struct

  let to_file hsc file =
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let dump =
        match Layout.get_dump ~dbh with
        | Ok dump ->
          Layout.sexp_of_dump dump |> Sexplib.Sexp.to_string_hum
        | Error (`layout_inconsistency 
                    (`file_system, `select_did_not_return_one_cache (tbl, l))) ->
          eprintf "get_dump detected a file system inconsistency: \n\
                   table %s returned %d caches for a given id" tbl l;
          failwith "Dumps.to_file"
        | Error (`layout_inconsistency _) -> (* TODO *)
          eprintf "get_dump detected a layout inconsistency:
                   UNKNOWN -- WORK IN PROGRESS \n";
          failwith "Dumps.to_file"
        | Error (`pg_exn e) ->
          eprintf "Getting the dump from the DB failed:\n  %s" 
            (Exn.to_string e);
          failwith "Dumps.to_file"
      in
      Out_channel.(with_file file ~f:(fun o -> output_string o dump));
      ignore (Hitscore_threaded.db_disconnect hsc dbh)
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
      
  let load_file hsc file =
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let insert_result = 
        In_channel.with_file file ~f:(fun i ->
          Sexplib.Sexp.input_sexp i |> Layout.dump_of_sexp |>
              Layout.insert_dump ~dbh) in
      begin match insert_result with 
      | Ok () -> eprintf "Load: Ok\n%!"
      | Error (`wrong_version (one, two)) ->
        eprintf "Load: Wrong Version Error: %S Vs %S\n%!" one two
      | Error (`layout_inconsistency
                  (`file_system, `insert_cache_did_not_return_one_id (table, ids))) ->
        eprintf "Load: insert_dump detected an inconsistency: inserting in %S \
                 returned more than one id: [%s]\n%!" 
          table (String.concat ~sep:"; " (List.map ids Int32.to_string))
      | Error (`layout_inconsistency _) -> (* TODO *)
        eprintf "get_dump detected a layout inconsistency:
                   UNKNOWN -- WORK IN PROGRESS \n";
        failwith "Dumps.to_file"
      | Error (`pg_exn e) ->
        eprintf "Load: Got a DB exception: %s\n" (Exn.to_string e)
      end;
      Hitscore_threaded.db_disconnect hsc dbh  |> ignore
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
        


end


module Hiseq_raw = struct

  let fs_checks directory run_params sum1 =
    begin 
      try 
        let dir_stat = Unix.stat directory in
        match dir_stat.Unix.st_kind with
        | Unix.S_DIR -> ()
        | _ ->
          eprintf "%s is not a directory\n" directory;
          failwith "Hiseq_raw.register"
      with
      | Unix.Unix_error _ ->
        eprintf "Cannot stat %s\n" directory;
        failwith "Hiseq_raw.register"
    end;
    begin
      try
        Unix.stat run_params |> ignore
      with 
      | Unix.Unix_error _ ->
        eprintf "Cannot stat %s\n" run_params;
        failwith "Hiseq_raw.register"
    end;
    begin
      try
        Unix.stat sum1 |> ignore
      with 
      | Unix.Unix_error _ ->
        eprintf "Cannot stat %s\n" sum1;
        failwith "Hiseq_raw.register"
    end

  let register ?host config directory =
    if not (Filename.is_absolute directory) then (
      eprintf "%s is not an absolute path...\n" directory;
      failwith "Hiseq_raw.register"
    );
    let xml_run_params = Filename.concat directory "runParameters.xml" in
    let xml_read1 = 
      Filename.concat directory "Data/reports/Summary/read1.xml" in
    fs_checks directory xml_run_params xml_read1;

    let { Hitscore_interfaces.Hiseq_raw_information.
          flowcell_name     ; 
          read_length_1     ; 
          read_length_index ; 
          read_length_2     ; 
          with_intensities  ; 
          run_date          ; } =
      let xml = 
        In_channel.with_file xml_run_params ~f:(fun ic ->
          XML.(make_input (`Channel ic) |> in_tree)) in
      match Hitscore_threaded.Hiseq_raw.run_parameters (snd xml) with
      | Ok t -> t
      | Error (`parse_run_parameters (`wrong_date s)) ->
        failwithf "Error while parsing date in runParameters.xml: %s" s ()
      | Error (`parse_run_parameters (`wrong_field s)) ->
        failwithf "Error while parsing %s in runParameters.xml" s ()
    in

    let host = Option.value ~default:"bowery.es.its.nyu.edu" host in
    match Hitscore_threaded.db_connect config with
    | Ok dbh ->
      let hs_raw =
        Hitscore_threaded.Layout.Record_hiseq_raw.add_value ~dbh
          ~flowcell_name
          ~read_length_1:(Int32.of_int_exn read_length_1)
          ?read_length_2:(Option.map read_length_2 Int32.of_int_exn)
          ?read_length_index:(Option.map read_length_index Int32.of_int_exn)
          ~with_intensities ~run_date ~host
          ~hiseq_dir_name:(Filename.basename directory)
      in
      begin match hs_raw with
      | Ok in_db ->
        Hitscore_threaded.db_disconnect config dbh  |> ignore;
        eprintf "The HiSeq raw directory was successfully added as %ld\n"
        in_db.Hitscore_threaded.Layout.Record_hiseq_raw.id
      | Error (`layout_inconsistency (_,
                                      `insert_did_not_return_one_id (s, i32l))) ->
        eprintf "ERROR: Layout Inconsistency Detected: \n\
                  insert in %s did not return one id but %d\n"
          s (List.length i32l);
        failwith "Hiseq_raw.register"
      | Error (`pg_exn e) ->
        eprintf "ERROR: from PGOCaml:\n%s\n" (Exn.to_string e);
        failwith "Hiseq_raw.register"
      end
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)

  let get_info directory = 
    let xml_run_params = Filename.concat directory "runParameters.xml" in
    let xml_read1 = 
      Filename.concat directory "Data/reports/Summary/read1.xml" in
    let { Hitscore_interfaces.Hiseq_raw_information.
          flowcell_name     ; 
          read_length_1     ; 
          read_length_index ; 
          read_length_2     ; 
          with_intensities  ; 
          run_date          ; } =
      let xml = 
        In_channel.with_file xml_run_params ~f:(fun ic ->
          XML.(make_input (`Channel ic) |> in_tree)) in
      match Hitscore_threaded.Hiseq_raw.run_parameters (snd xml) with
      | Ok t -> t
      | Error (`parse_run_parameters (`wrong_date s)) ->
        failwithf "Error while parsing date in runParameters.xml: %s" s ()
      | Error (`parse_run_parameters (`wrong_field s)) ->
        failwithf "Error while parsing %s in runParameters.xml" s ()
    in
    let clustering =
      let xml = 
        In_channel.with_file xml_read1 ~f:(fun ic ->
          XML.(make_input (`Channel ic) |> in_tree)) in
      match Hitscore_threaded.Hiseq_raw.clusters_summary (snd xml) with
      | Ok t -> t
      | Error (`parse_clusters_summary s) ->
        failwithf "Error while parsing read1.xml: %s" s ()
    in
    printf "FCID: %s (%d%s%s from the %s run, %s intensities)\n"
      flowcell_name
      read_length_1
      (Option.value_map ~default:"" ~f:(sprintf "x%d") read_length_index)
      (Option.value_map ~default:"" ~f:(sprintf "x%d") read_length_2)  
      (run_date |! Time.to_local_date |! Date.to_string)
      (if with_intensities then "with" else "without");
    let head = printf " |% 19s" in
    printf "Lane";
    head "clusters_raw";      
    head "clusters_raw_sd";   
    head "clusters_pf";
    head "clusters_pf_sd";
    head "prc_pf_clusters";
    head "prc_pf_clusters_sd";
    printf "\n";
    Array.iteri clustering ~f:(fun i a ->
      let open Hitscore_interfaces.Hiseq_raw_information in
      printf "  %d " (i + 1);
      match a with
      | None -> printf "-- NOT AVAILABLE --"
      | Some c ->
        let cell = printf " |% 19.2f" in
        cell c.clusters_raw;      
        cell c.clusters_raw_sd;   
        cell c.clusters_pf;
        cell c.clusters_pf_sd;
        cell c.prc_pf_clusters;
        cell c.prc_pf_clusters_sd;
        printf "\n"
    );
    ()

end

module Verify = struct


  let check_file_system ?(try_fix=false) ?(verbose=true) hsc =
    let buffer = ref [] in
    let log fmt =
      let f s = buffer := (`info s) :: !buffer in
      ksprintf f fmt in
    let error kind fmt =
      let f s = buffer := (`error (kind, s)) :: !buffer in
      ksprintf f fmt in
    begin match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let (>>>) x f = Result.ok_exn ~fail:(Failure f) x in
      let vols = Layout.File_system.get_all ~dbh >>> "File_system.get_all" in
      List.iter vols ~f:(fun volume -> 
        let path =
          Hitscore_threaded.Common.path_of_volume ~dbh ~configuration:hsc
            volume >>> "Getting path"
        in
        if verbose then
          log "* Checking volume %S\n" path;
        Unix.(try
                let vol_stat = stat path in
                if vol_stat.st_kind <> S_DIR then (
                  error `volume "%S: Not a directory" path;
                ) else
                  if verbose then
                    log "-> OK\n"
                  else
                    ()
          with
          | Unix_error (e, _, s) ->
            error `volume "%S: %S (%S)" path (error_message e) s;
            if try_fix then
              begin match ksprintf System.command "mkdir -p %s" path with
              | Ok () ->
                begin match 
                    Hitscore_threaded.Access_rights.set_posix_acls ~dbh 
                      ~configuration:hsc (`dir path) with
                      | Ok () -> log "Fixed %S\n" path;
                      | Error _ -> error `fix "%S: Cannot set ACLs" path
                end
              | Error _ -> error `fix "%S: Cannot mkdir" path
              end);
        let all_paths =
          Hitscore_threaded.Common.all_paths_of_volume ~dbh ~configuration:hsc
            volume >>> "Getting All paths" in
        List.iter all_paths ~f:(fun filename -> 
          if verbose then log "  \\-> %S\n" filename;
          Unix.(
            try let file_stat = stat filename in
                ignore file_stat
            with
            | Unix_error (e, _, s) ->
              error `file "%S: %S (%S)" filename (error_message e) s);
        ))
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
    end;
    List.rev !buffer

  let print_fs_check =
    List.iter  ~f:(function
    | `info s -> printf "%s" s
    | `error (`fix, s) -> printf "ERROR(fixing/mkdir): %s" s
    | `error (`volume, s) -> printf "ERROR(volume): %s\n" s
    | `error (`get_files, s) -> printf "ERROR(get-files): %s\n" s
    | `error (`file, s) -> printf "ERROR(file): %s\n" s)

  let check_duplicates configuration =
    let open Hitscore_threaded in
    let open Result_IO in
    let work_samples =
      with_database ~configuration ~f:(fun ~dbh ->
        Layout.Record_sample.(
          get_all ~dbh
          >>= fun all ->
          of_list_sequential all ~f:(fun p ->
            get ~dbh p
            >>= fun {name; project; _} ->
            return (name, project))
          >>= fun all_names_and_projects ->
          return all_names_and_projects))
    in
    let work_libraries =
      with_database ~configuration ~f:(fun ~dbh ->
        Layout.Record_stock_library.(
          get_all ~dbh
          >>= fun all ->
          of_list_sequential all ~f:(fun p ->
            get ~dbh p
            >>= fun {name; project; _} ->
            return (name, project))
          >>= fun all_names_and_projects ->
          return all_names_and_projects))
    in
    let check work what =
      match work with
      | Ok l ->
        List.iter (List.sort ~cmp:compare l) (fun (name, project) ->
        (* printf "%s . %s\n" (Option.value ~default:"" project) name; *)
          let all = (List.find_all l ~f:(fun (n, p) -> n = name && p <> project)) in
          match all with
          | []  -> ()
          | more ->
            printf "Duplicate %s: %S . %S " what
              (Option.value ~default:"" project) name;
            List.iter more (fun (n, p) ->
              printf " ==  %S . %S" (Option.value ~default:"" p) n;);
            printf "\n")
      | Error e ->
        printf "ERROR"
    in
    check work_samples "sample";
    check work_libraries "stock-library";
    ()
    
  let wake_up ?(fix_it=false) hsc =
    let open Hitscore_threaded in
    begin match db_connect hsc with
    | Ok dbh ->
      let count_fs_errors =
        List.fold_left (check_file_system ~verbose:false hsc)
          ~init:0 ~f:(fun x -> function `info _ -> x | `error _ -> x + 1) in
      begin match count_fs_errors with
      | 0 -> printf "File-system: OK.\n"; 
      | n -> printf "File-system: %d errors.\n" n
      end;
      let () =
        let open Layout.Function_bcl_to_fastq in
        let started_b2fs =
          match get_all_started ~dbh with
          | Ok l -> l
          | Error (`pg_exn e) -> 
            failwithf "B2F.get_all_started: %S" (Exn.to_string e) ()
        in
        List.iter started_b2fs (fun bcl_to_fastq ->
          let status = 
            Bcl_to_fastq.status ~dbh ~configuration:hsc bcl_to_fastq in
          match status with
          | Ok `running -> ()
          | Ok (`started_but_not_running e) -> 
            printf "The function %ld is STARTED BUT NOT RUNNING!!!\n"
              bcl_to_fastq.id;
            if fix_it then (
              printf "Fixing …\n";
              Bcl_to_fastq.fail ~dbh bcl_to_fastq
                ~reason:"checking_status_reported_started_but_not_running"
              |! ignore)
          | Ok (`not_started e) ->
            printf "ERROR: The function %ld is NOT STARTED: %S.\n"
              bcl_to_fastq.id
              (Layout.Enumeration_process_status.to_string e);
          | Error (`pg_exn e) -> 
            printf "ERROR B2F.status: %S\n" (Exn.to_string e) 
          | Error (`status_parsing_error e) ->
            printf "ERROR B@f.status: status_parsing_error %S\n" e 
          | Error (`work_directory_not_configured) ->
            printf "ERROR B2F.status: work_directory_not_configured\n" 
          | Error _ ->
            failwithf "B2F.status ERROR" ()
        )
      in
      db_disconnect hsc dbh |! ignore

    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
    end;
    check_duplicates hsc;
    Some ()


end

module FS = struct

  let add_files_to_volume hsc vol files =
    failwith "Not implemented any more"
     (* 
    let open Hitscore_threaded.Result_IO in
    let volume_path = Hitscore_threaded.Configuration.path_of_volume_fun hsc |>
        Option.value_exn_message "Configuration has no root directory" in 
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      Hitscore_threaded.Layout.File_system.(
        let vol_cache = get_volume ~dbh (unsafe_cast vol) in
        begin match vol_cache with 
        | Ok vc ->
          let path = entry_unix_path vc.volume_entry in
          let file_args = 
            String.concat ~sep:" " (List.map files (sprintf "%S")) in
          eprintf "Copying %s to %s/\n" file_args (volume_path path);
          ksprintf System.command_exn "cp %s %s/" file_args (volume_path path)
        | Error (`layout_inconsistency (`file_system,
                                        `select_did_not_return_one_tuple (s, i))) ->
          eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.get_volume detected an inconsistency\n\
          FILE_SYSTEM: select_did_not_return_one_tuple (%s, %d)" s i;
          failwith "STOP"
        | Error (`pg_exn e) ->
          eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.cache_volume had a PGOCaml error:\n%s"
            (Exn.to_string e);
          failwith "STOP"
        end;
        eprintf "Copy: DONE (won't go back).\n";
        begin match
            add_tree_to_volume ~dbh (unsafe_cast_volume vol) 
              (List.map files (fun f -> Tree.file (Filename.basename f))) with
        | Ok () ->
          begin match
            Hitscore_threaded.Layout.Record_log.add_value ~dbh
              ~log:(sprintf "(add_files_to_volume %ld (%s))" vol
                      (String.concat ~sep:" " 
                         (List.map files (sprintf "%S")))) with
              | Ok _ -> 
                eprintf "add_tree_to_volume: OK\n"
              | Error _ ->
                eprintf "add_tree_to_volume: OK (but logging problem?)\n"
          end
        | Error (`layout_inconsistency (`file_system,
                                        `add_did_not_return_one (s, l))) ->
          eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.add_tree_to_volume detected an inconsistency\n\
          FILE_SYSTEM: add_did_not_return_one (%s, [%s])"
            s (String.concat ~sep:"; " (List.map l Int32.to_string))
      | Error (`pg_exn e) ->
        eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.add_tree_to_volume had a PGOCaml error:\n%s"
          (Exn.to_string e)
        end
      )
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
     *)

end


module Flowcell = struct
  open Hitscore_threaded
  open Result_IO

  let check_lane_unused ~dbh lane = 
    let q_res =
      let module PGOCaml = Hitscore_threaded.Layout.PGOCaml in
      let open Batteries in
      let lanes = [| lane |] in
      PGSQL (dbh)
        "select g_id, serial_name from flowcell
         where flowcell.lanes @> $lanes"
    in
    if List.length q_res <> 0 then (
      List.iter q_res (fun (id, fcid) ->
        failwithf "Lane %ld already used in flowcell %ld (%S)" lane id fcid ()))
    else
      ()

  let checks_and_read_lengths ~dbh lanes =
    if List.length (List.dedup lanes) < List.length lanes then
      failwithf "Cannot reuse same lane" ();
    let lanes_r1_r2 =
      List.map lanes (fun id ->
        check_lane_unused ~dbh id;
        Layout.Record_lane.(
          get ~dbh (unsafe_cast id)
          >>= fun { requested_read_length_1; requested_read_length_2; _ } ->
          return (requested_read_length_1, requested_read_length_2 )
        ) |! function
        | Ok (a, b) -> (a, b)
        | Error e -> 
          printf "ERROR while checking lane %ld\n" id;
          failwith "ERROR") in
    let first_r1, first_r2 = List.hd_exn lanes_r1_r2 in
    let all_equal =
      List.for_all lanes_r1_r2 
        ~f:(fun (r1, r2) -> r1 = first_r1 && r2 = first_r2) in
    if not all_equal then (
      printf "ERROR: Requested read-lengths are not all equal: [\n%s]\n"
        (List.mapi lanes_r1_r2 (fun i (r1, r2) ->
          match r2 with
          | None -> sprintf "  %d : SE %ld\n" (i + 2) r1
          | Some r -> sprintf "  %d : PE %ldx%ld\n" (i + 2) r1 r)
          |! String.concat ~sep:"");
      failwith "ERROR"
    );
    (first_r1, first_r2)

  let new_input_phix ~dbh r1 r2 =
    begin match Layout.Search.record_stock_library_by_name ~dbh "PhiX_v3" with
    | Ok [library] ->
      let input_phix =
        Layout.Record_input_library.add_value ~dbh
          ~library ~submission_date:(Time.now ())
          ?volume_uL:None ?concentration_nM:None ~user_db:[| |] ?note:None
      in
      begin match input_phix with
      | Ok i_phix ->
        Layout.Record_log.add_value ~dbh
          ~log:(sprintf "(add_input_library_phix %ld)"
                  i_phix.Layout.Record_input_library.id)
        |! Pervasives.ignore;
        let libraries = [| i_phix |] in
        let phix_lane = Layout.Record_lane.add_value ~dbh
          ~libraries  ?total_volume:None ?seeding_concentration_pM:None
          ~pooled_percentages:[| 100. |]
          ~requested_read_length_1:r1 ?requested_read_length_2:r2
          ~contacts:[| |] in
        begin match phix_lane with
        | Ok l_phix ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(add_lane_phix %ld)"
                    l_phix.Layout.Record_lane.id) |! Pervasives.ignore;
          l_phix
        | Error e ->
          failwithf "Could not create the lane for PhiX.\n" ();
            (* TODO delete input_library *)
        end
      | Error e ->
        failwithf "ERROR: Cannot add input_library for PhiX\n" ();
      end
    | _ ->
      failwithf "ERROR: Could not find PhiX_v3\n" ();
    end

  let new_empty_lane ~dbh r1 r2 =
    let lane = 
      Layout.Record_lane.add_value ~dbh
        ~libraries:[||]
        ?total_volume:None ?seeding_concentration_pM:None
        ~pooled_percentages:[| |]
        ~requested_read_length_1:r1 ?requested_read_length_2:r2
        ~contacts:[||] in
    begin match lane with
    | Ok l ->
      Layout.Record_log.add_value ~dbh
        ~log:(sprintf "(add_empty_lane %ld)" l.Layout.Record_lane.id)
      |! Pervasives.ignore;
      l
    | Error e ->
      failwithf "Could not create the empty lane.\n" ();
    end


  let parse_args =
    List.map ~f:(function
    | "PhiX" | "phix" | "PHIX" -> `phix
    | "Empty" | "empty" | "EMPTY" -> `empty
    | x -> 
      let i = try Int32.of_string x with e ->
          failwithf "Cannot understand arg: %S (should be an integer)" x () in
      `lane i)


  let register hsc name args =
    if List.length args <> 8 then
      failwith "Expecting 8 arguments after the flowcell name.";
    let lanes = parse_args args in
    match db_connect hsc with
    | Ok dbh ->
      begin match Layout.Search.record_flowcell_by_serial_name ~dbh name with
      | Ok [] ->
        printf "Registering %s\n" name;
        let r1, r2 = 
          checks_and_read_lengths ~dbh 
            (List.filter_map lanes (function `lane i -> Some i | _ -> None)) in
        printf "It is a %S flowcell\n"
          (match r2 with 
          | Some s -> sprintf "PE %ldx%ld" r1 s
          | None -> sprintf "SE %ld" r1);
        let lanes =
          List.map lanes ~f:(function
          | `phix ->  new_input_phix ~dbh r1 r2 
          | `lane id -> Layout.Record_lane.unsafe_cast id
          | `empty -> new_empty_lane ~dbh r1 r2)
          |! Array.of_list in
        let flowcell = 
          Layout.Record_flowcell.add_value ~dbh ~serial_name:name ~lanes in
        begin match flowcell with
        | Ok f ->
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(add_flowcell %ld %s (lanes %s))"
                    f.Layout.Record_flowcell.id name
                    (List.map (Array.to_list lanes) (fun i ->
                      sprintf "%ld" i.Layout.Record_lane.id)
                      |! String.concat ~sep:" "))
          |! Pervasives.ignore;
        | Error e ->
          printf "Could not add flowcell: %s\n" name
              (* TODO: manage input_lib and lane *)
        end
      | Ok [one] ->
        printf "ERROR: Flowcell name %S already used.\n" name;
      | Ok l ->
        printf "BIG-ERROR: Flowcell name %S already used %d times!!!\n" 
          name (List.length l);
      | Error _ ->
        printf "Database error"
      end;
      db_disconnect hsc |! Pervasives.ignore
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)


end

module Query = struct

  module PGOCaml = Hitscore_threaded.Layout.PGOCaml

  let predefined_queries = [
    ("orphan-lanes", 
     (["List of lanes which are not referenced by any flowcell."],
      fun dbh args ->
        let lanes =
          let open Batteries in
          PGSQL (dbh)
            "select lane.g_id, 
                    lane.requested_read_length_1, lane.requested_read_length_2,
                    invoicing.pi
             from (lane left outer join flowcell 
                    on flowcell.lanes @> array_append ('{}', lane.g_id))
                  left outer join invoicing on
                   invoicing.lanes @> array_append('{}', lane.g_id)
             where flowcell.serial_name is NULL and invoicing.pi is not NULL;"
          @ (
            PGSQL (dbh)
            "select lane.g_id, 
                    lane.requested_read_length_1, lane.requested_read_length_2
             from (lane left outer join flowcell 
                    on flowcell.lanes @> array_append ('{}', lane.g_id))
                  left outer join invoicing on
                   invoicing.lanes @> array_append('{}', lane.g_id)
             where flowcell.serial_name is NULL and invoicing.pi is NULL;"
              |> List.map (fun (x,y,z) -> (x,y,z,0l)))
        in
        let person_string p =
          let open Batteries in
          PGSQL (dbh) "select family_name from person where g_id = $p"
          |> function
            | [] -> "NO-INVOICED-PI"
            | [one] -> one
            | more -> String.concat "--" more
        in
        let run_type r1 = function
          | Some r2 -> sprintf "PE %ldx%ld" r1 r2
          | None -> sprintf "SE %ld" r1 in
        let fixed =
          List.sort lanes ~cmp:(fun (x, _, _ , _) (y, _, _ , _) -> compare x y)
          |! List.fold_left ~init:(0l, "", 0) 
              ~f:(fun (c, s, cpt) (x, r1, r2, pi) ->
                if x = c then
                  (x, sprintf "%s, %s" s (person_string pi), cpt)
                else
                  (x, sprintf "%s\n * %ld (%s) -> %s" s x 
                    (run_type r1 r2) (person_string pi), 
                   cpt + 1))
        in
        begin match fixed with
        | (_, _, 0) -> printf "No orphan lanes found.\n"
        | (_, s, n) ->
          printf "Found %d orphan lanes:%s\n" n s
        end));

    ("log",
     (["Display the log table.";
       "usage: log [<number of items>]"],
      fun dbh args ->
        let logs =
          let open Batteries in
          PGSQL (dbh)
            " select log.g_last_modified, log.log from log 
              order by g_last_modified"
        in
        begin match List.length logs with
        | 0 -> printf "No logs found.\n"
        | n ->
          let filtered = 
            match args with
            | [] -> logs
            | [ n ] -> 
              begin try 
                      let n = List.length logs - Int.of_string n  in
                      List.split_n logs n |! snd
                with e -> failwithf "Can't understand argument: %s" n ()
              end
            | _ -> failwithf "Wrong arguments!" ()
          in
          List.iter filtered (function
          | (Some t, l) ->
            printf "[%s] %s\n" t l
          | None, l ->
            printf "[???NO DATE???] %s\n" l
          );
        end));
    ("orphan-input-libraries", 
     (["List of libraries which are not referenced in any lane."],
      fun dbh args ->
        let libs =
          let open Batteries in
          PGSQL (dbh)
            "select input_library.g_id, 
                    stock_library.project,
                    stock_library.name
             from (input_library left outer join lane 
                    on lane.libraries @> array_append ('{}', input_library.g_id)),
                  stock_library
             where lane.g_id is NULL and input_library.library = stock_library.g_id;"
        in
        let fixed = libs in
        begin match List.length fixed with
        | 0 -> printf "No orphan input-libraries found.\n"
        | n ->
          printf "Found %d orphan input-librar%s:\n%s\n" n 
            (if n > 1 then "ies" else "y")
            (String.concat ~sep:"\n" 
               (List.map fixed
                  (fun (i, p, n) ->
                    sprintf " * %ld: %s%S" i
                      (Option.value_map p ~default:"" ~f:(sprintf "%S.")) n)))
        end));
    ("flowcells-csv",
     (["A CSV output, imitating the Flowcell in the Meta-data."],
     fun dbh args ->
       let work =
         let open Hitscore_threaded in
         let open Result_IO in
         Layout.Record_flowcell.(
           get_all ~dbh
           >>= fun flowcells ->
           of_list_sequential flowcells ~f:(fun f ->
             get ~dbh f >>= fun {serial_name; lanes} ->
             of_list_sequential
               (Array.to_list (Array.mapi lanes ~f:(fun i a -> (i,a))))
               ~f:(fun (i, l) ->
                 Layout.Record_lane.(
                   get ~dbh l >>= fun {libraries; _} ->
                   of_list_sequential (Array.to_list libraries) ~f:(fun lib ->
                     Layout.Record_input_library.(
                       get ~dbh lib >>= fun {library; _} ->
                       Layout.Record_stock_library.(
                         get ~dbh library >>= fun {name; _} ->
                         return (serial_name, i + 1, name))))))))
         >>= fun ll ->
         return (List.iter (List.flatten (List.flatten ll)) (fun (s, i, n) ->
           printf "%S,%d,%s\n" s i n
         )) in
       ignore work
           
     ));
    ("b2f-volumes", 
     (["List of B2F results."],
      fun dbh args ->
        let results =
          let open Batteries in
          PGSQL (dbh)
            "select bcl_to_fastq.g_id as B2F_id,
                    hiseq_raw.flowcell_name as FCID,
                    bcl_to_fastq.tiles as TILES,
                    g_volume.g_id as VOL_id,
                    g_volume.g_sexp
             from bcl_to_fastq, bcl_to_fastq_unaligned, g_volume, hiseq_raw
             where bcl_to_fastq.g_result = bcl_to_fastq_unaligned.g_id AND
                   bcl_to_fastq_unaligned.directory = g_volume.g_id AND
                   bcl_to_fastq.raw_data = hiseq_raw.g_id;"
        in
        begin match List.length results with
        | 0 -> printf "No results found.\n"
        | n ->
          printf "Found %d result%s:\n%s\n" n 
            (if n > 1 then "s" else "")
            (String.concat ~sep:"\n" 
               (List.map results
                  (fun (i, fc, til, v, sexp) ->
                    sprintf " % 4ld: %- 10s %- 40S --> % 4ld %s" i fc
                      (Option.value ~default:"N/A" til) v sexp)))
        end));
    ("invoices", 
     (["List of invoices."],
      fun dbh args ->
        let results =
          let open Batteries in
          PGSQL (dbh)
            "select invoicing.g_id, person.family_name, flowcell.serial_name 
             from invoicing, person, flowcell
             where invoicing.pi = person.g_id
              and flowcell.lanes @> invoicing.lanes "
        in
        begin match List.length results with
        | 0 -> printf "No results found.\n"
        | n ->
          printf "Found %d result%s:\n%s\n" n 
            (if n > 1 then "s" else "")
            (List.map results (fun (inv, pi, fcid) ->
              sprintf "% 4ld | % 20s | % 20s" inv pi fcid)
              |! String.concat ~sep:"\n")
        end));
    ("deliveries",
     (["List of potential deliveries for a flowcell"],
      fun dbh args ->
        let fcid =
          match args with [one] -> one | _ -> failwith "expecting one argument" in
        let invoices =
          let open Batteries in
          PGSQL (dbh)
            "select invoicing.g_id, person.family_name
             from invoicing, person, flowcell
             where invoicing.pi = person.g_id
              and flowcell.lanes @> invoicing.lanes
              and flowcell.serial_name = $fcid"
        in
        let b2fs =
          let open Batteries in
          PGSQL (dbh)
            "select bcl_to_fastq.g_id as B2F_id,
                    bcl_to_fastq.tiles as TILES,
                    g_volume.g_id as VOL_id,
                    g_volume.g_sexp
             from bcl_to_fastq, bcl_to_fastq_unaligned, g_volume, hiseq_raw
             where bcl_to_fastq.g_result = bcl_to_fastq_unaligned.g_id AND
                   bcl_to_fastq_unaligned.directory = g_volume.g_id AND
                   bcl_to_fastq.raw_data = hiseq_raw.g_id AND
                   hiseq_raw.flowcell_name = $fcid;"
        in
        begin match List.length invoices with
        | 0 -> printf "No invoices found.\n"
        | n ->
          printf "Found %d invoice%s:\n%s\n" n 
            (if n > 1 then "s" else "")
            (List.map invoices
               (fun (inv, pi) -> sprintf "% 4ld (to %s)" inv pi)
                                 |! String.concat ~sep:"\n")
        end;
        begin match List.length b2fs with
        | 0 -> printf "No bcl-to-fastqs found.\n"
        | n ->
          printf "Found %d bcl-to-fastqs%s:\n%s\n" n 
            (if n > 1 then "s" else "")
            (List.map b2fs (fun (id, tiles, volid, sexp) ->
              sprintf "% 4ld (tiles: %s, result: %ld/%s)" id
                      (Option.value ~default:"N/A" tiles) volid sexp)
              |! String.concat ~sep:"\n")
        end;
     ));
  ]

  let describe out =
    List.iter predefined_queries ~f:(fun (name, (desc, _)) ->
      fprintf out " * %S:\n        %s\n" name
        (String.concat ~sep:"\n        " desc))

  let predefined hsc name args =
    let open Hitscore_threaded in
    match db_connect hsc with
    | Ok dbh ->
      begin match List.Assoc.find predefined_queries name with
      | Some (_, run) -> run dbh args
      | None ->
        printf "Unknown custom query: %S\n" name;
      end;
      db_disconnect hsc dbh |! Pervasives.ignore
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)


end

module Prepare_delivery = struct

  let run_function configuration bb inv dir directory_tag =
    let open Hitscore_threaded in
    let open Result_IO in
    let out fmt = ksprintf (fun s -> (eprintf "%s" s)) fmt in
    if not (Filename.is_absolute dir)
    then (eprintf "%s is not an absolute path" dir; failwith "STOP");
    let work =
      db_connect configuration >>= fun dbh ->
      Unaligned_delivery.run ~dbh ~configuration ?directory_tag
        ~bcl_to_fastq:(Layout.Function_bcl_to_fastq.unsafe_cast
                         (Int32.of_string bb)) 
        ~invoice:(Layout.Record_invoicing.unsafe_cast (Int32.of_string inv))
        ~destination:dir
      >>= fun preparation ->
      out "Done: Preparation: %ld\n" 
        preparation.Layout.Function_prepare_unaligned_delivery.id;
      db_disconnect configuration dbh
    in
    match work with
    | Ok () -> out "OK\n"
    | Error e ->
      begin match e with
      | `bcl_to_fastq_not_succeeded (fpointer, status) ->
        out "Fun %ld is not succeeded: %s\n"
          fpointer.Layout.Function_bcl_to_fastq.id
          (Layout.Enumeration_process_status.to_string status)
      | `cannot_recognize_file_type s ->
        out "Wrong file type: %s\n" s
      | `inconsistency_inode_not_found ld ->
        out "Inode not found: %ld\n" ld
      | `io_exn e ->
        out "I/O exception: %s\n" (Exn.to_string e)
      | `layout_inconsistency (where, what) ->
        out "Layout problem: In ";
        begin match where with
        | `File_system -> "File system"
        | `Function f -> sprintf "Function %s" f
        | `Record r -> sprintf "Record %s" r
        end |! out "%s, ";
        begin match what with
        | `select_did_not_return_one_tuple (s, d) ->
          out "not single response for the pointer:  %S, %d\n" s d
        | `insert_did_not_return_one_id (s, l) ->
          out "insert did not return one id: %s, [%s]\n" s 
            (List.map l ~f:(sprintf "%ld") |! String.concat ~sep:", ")
        end
      | `partially_found_lanes (g_id, serial_name,
                                invoice_lanes, find_lanes) ->
        out "Partially found lanes in flowcell: %ld (%s), [%s] Vs [%s]\n"
          g_id serial_name
          (Array.to_list invoice_lanes 
           |! List.map ~f:(fun il -> il.Layout.Record_lane.id |! sprintf "%ld")
           |! String.concat ~sep:", ")
          (List.map (List.filter_opt find_lanes) ~f:(sprintf "%d") 
           |! String.concat ~sep:", ") 
      | `not_single_flowcell [] ->
        out "Did not find any matching flowcell\n"
      | `not_single_flowcell l ->
        out "Found too many flowcells: %s\n"
          (List.map l ~f:(fun (s, l) -> s) |! String.concat ~sep:"; ")
      | `pg_exn e ->
        out "PostgreSQL Error: %s\n" (Exn.to_string e)
      | `root_directory_not_configured ->
        out "work-dir not configured\n"
      | `wrong_unaligned_volume (sl) ->
        out "Unaligned volume not conform to standards: [%s]\n"
          (List.map sl (sprintf "%S") |! String.concat ~sep:", ")
      | `system_command_error (c, e) ->
        out "Error with system command:\n  %s\n  %s\n" c (Exn.to_string e)

      end
        
end

module Intensities_deletion = struct
  let do_registration configuration dir =
    let open Hitscore_threaded in
    let open Result_IO in
    let out fmt = ksprintf (fun s -> (eprintf "%s" s)) fmt in
    let work =
      with_database configuration (fun ~dbh ->
        Delete_intensities.register ~dbh
          ~hiseq_raw: (Layout.Record_hiseq_raw.unsafe_cast (Int32.of_string dir)) 
      ) in
    match work with
    | Ok _ -> out "OK\n"
    | Error e ->
      begin match e with
      | `pg_exn e ->
        out "PostgreSQL Error: %s\n" (Exn.to_string e)
      | `hiseq_dir_deleted ->
        out "Trying to access a deleted HiSeq-raw\n"
      | `layout_inconsistency _ ->
        out "LAYOUT INCONSISTENCY"
      end
end 

module Hiseq_run = struct

  let register configuration day_date fca fcb note =
    let open Hitscore_threaded in
    let open Result_IO in
    with_database configuration (fun ~dbh ->
      let flowcell id =
        if id = "_" then return None else
          try
            return (Some (Int32.of_string id |! Layout.Record_flowcell.unsafe_cast))
          with e ->
            (Layout.Search.record_flowcell_by_serial_name ~dbh id
             >>= function
             | [one] -> return (Some one)
             | _ -> failwithf "Error while looking for flowcell %s" id ())
      in
      flowcell fca >>= fun flowcell_a ->
      flowcell fcb >>= fun flowcell_b ->
      Layout.Record_hiseq_run.add_value
        ~dbh ~date:(Time.of_string (day_date ^ " 10:00"))
        ?flowcell_a ?flowcell_b ?note)
    |! Result.ok_exn ~fail:(Failure "adding the hiseq_run went wrong")
    |! Pervasives.ignore


end




  
let commands = ref []

let define_command ~names ~description ~usage ~run =
  commands := (names, description, usage, run) :: !commands

let short_help indent =
  String.concat ~sep:"\n"
    (List.map !commands
       (fun (names, desc, _, _) ->
         sprintf "%s * %s:\n%s%s%s" indent (String.concat ~sep:"|" names) 
           indent indent desc))

let find_command cmd = 
  List.find !commands (fun (names, _, _, _) -> List.mem cmd ~set:names)

let () =
  define_command 
    ~names:["all-barcodes-sample-sheet"; "abc"]
    ~description:"Make a sample sheet with all barcodes"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s <flowcell id>  <list lanes/barcode vendors>\n" 
        exec cmd;
      fprintf o "  example: %s %s D03NAKCXX N1 I2 I3 B4 B5 I6 I7 N8\n" exec cmd;
      fprintf o "  where N1 means no barcode on lane 1, I2 means all \
        Illumina barcodes on lane 2,\n  B4 means all BIOO barcodes on lane \
        4, etc.\n";)
    ~run:(fun config exec cmd -> function
      | flowcell :: specification ->
        Some (All_barcodes_sample_sheet.make ~flowcell ~specification print_string) 
      | _ -> None);
  
  define_command
    ~names:[ "pbs"; "make-pbs" ]
    ~description:"Generate PBS scripts"
    ~usage:(fun o exec cmd -> 
      fprintf o "usage: %s <profile> %s [OPTIONS] <script-names>\nsee: %s %s -help\n"
        exec cmd exec cmd)
    ~run:(fun config exec cmd _ ->
      Some (PBS_script_generator.parse_cmdline (sprintf "%s %s" exec cmd) 1));

  define_command
    ~names:["gb2f"; "gen-bcl-to-fastq"]
    ~usage:(fun o exec bcl2fastq ->
      fprintf o  "usage: %s profile %s name basecalls-dir sample-sheet\n" 
        exec bcl2fastq)
    ~description:"Prepare a BclToFastq run"
    ~run:(fun config exec cmd -> function
      | [ name; basecalls; sample_sheet ] ->
        Some (Gen_BclToFastq.prepare name basecalls sample_sheet)
      | _ -> None);

  define_command
    ~names:["dump-to-file"]
    ~description:"Dump the database to a S-Exp file"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s <filename>\n" exec cmd)
    ~run:(fun config exec cmd -> function
      | [file] -> Some (Dumps.to_file config file)
      | _ -> None);

  define_command
    ~names:["load-file"]
    ~description:"Load a dump the database (S-Exp file)"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s <filename>\n" exec cmd)
    ~run:(fun config exec cmd -> function
      | [file] -> Some (Dumps.load_file config file)
      | _ -> None);

  define_command 
    ~names:["register-hiseq-raw"; "rhr"]
    ~description:"Register a HiSeq raw directory"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s [-host <host-addr>] <absolute-path>\n" 
        exec cmd;
      fprintf o "   (default host being bowery.es.its.nyu.edu)\n")
    ~run:(fun config exec cmd -> function
      | [path] -> Some (Hiseq_raw.register config path)
      | ["-host"; host; path] -> Some (Hiseq_raw.register config ~host path)
      | _ -> None);
 

  define_command
    ~names:["check-file-system"; "check-fs"]
    ~description:"Check the files registered in the database"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s [-quiet|-try-fix]\n" exec cmd;
      fprintf o "  -quiet : Non-verbose output\n";
      fprintf o "  -try-fix: Try to fix fixable errors\n"
    )
    ~run:(fun config exec cmd -> function
      | [] -> Some Verify.(check_file_system config |! print_fs_check)
      | [ "-quiet" ] -> Some Verify.(
        check_file_system ~verbose:false config |! print_fs_check)
      | [ "-try-fix" ] -> Some Verify.(
        check_file_system ~try_fix:true config |! print_fs_check)
      | _ -> None);

  define_command
    ~names:["add-files-to-volume"; "afv"]
    ~description:"Move files to a volume (at its root)"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s <volume_id:int> <file1> <file2> ...\n" 
        exec cmd)
    ~run:(fun config exec cmd -> function
      | vol :: files ->
        begin 
          try 
            let v = Int32.of_string vol in
            Some (FS.add_files_to_volume config v files)
          with e -> 
            eprintf "Exception: %s\n" (Exn.to_string e); None
        end
      | _ -> None);

  define_command 
    ~names:["print-configuration"; "pc"]
    ~description:"Display the current profile (and environment)."
    ~usage:(fun o exec cmd -> fprintf o "usage: %s <profile> %s\n" exec cmd)
    ~run:(fun config exec cmd -> function
      | [] -> 
        printf "** Current configuration:\n";
        Configuration_file.print_config config;
        printf "** Environment:\n";
        Configuration_file.print_env ();
        Some ()
      | _ -> None);
  define_command 
    ~names:["with-env"; "wenv"]
    ~description:"Run a command (default \"bash\") with the current \
                  profile's environment."
    ~usage:(fun o exec cmd -> fprintf o "usage: %s <profile> %s [<cmd>]\n" exec cmd)
    ~run:(fun config exec cmd -> function
      | [] -> Configuration_file.export_env config "bash"; Some ()
      | [cmd] -> Configuration_file.export_env config cmd; Some ()
      | _ -> None);


  define_command
    ~names:["parse-submission-sheet"; "pss"]
    ~description:"Parse a submission sheet (CSV)"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s [-wet-run|-verbose] \
                [<PhiX-config>] <pss>\n" exec cmd;
      fprintf o "where PhiX-config is <pool-name>:<phix-percent>\n";
      fprintf o "example: %s dev %s -verbose A:1 C:4 pss_with_some_pools.csv\n"
        exec cmd)
    ~run:(fun config exec cmd -> function
      | [] -> None
      | l ->
        let dry_run = not (List.exists l ~f:((=) "-wet-run")) in
        let verbose = List.exists l ~f:((=) "-verbose") in
        let args = List.filter l ~f:(fun x -> x <> "-wet-run" && x <> "-verbose") in
        match List.split_n args (List.length args - 1) with
        | phix_raw, [ pss ] ->
          begin 
            try
              let phix =
                List.map phix_raw ~f:(fun s ->
                  match String.split s ~on:':' with
                  | [ name; percent ] -> ("Pool " ^ name, Int.of_string percent)
                  | _ -> eprintf "Can't understand %s\n" s; failwith "phix parsing") in
              Some (Hitscore_submission_sheet.parse ~dry_run ~verbose ~phix config pss)
            with
            | e -> eprintf "Exception: %s\n" (Exn.to_string e); None
          end
        | _, _ -> None);

  define_command
    ~names:["register-flowcell"]
    ~description:"Register a new flowcell (for existing lanes)"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <flowcell-name> <L1> <L2> .. <L8>\n"
        exec cmd;
      fprintf o "where Lx is 'PhiX', 'Empty', or an orphan lane's id.\n")
    ~run:(fun config exec cmd -> function
    | [] -> None
    | name :: lanes ->
      Some (Flowcell.register config name lanes));

  define_command
    ~names:["query"; "Q"]
    ~description:"Query the database with (predefined) queries"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s <query> [<query arguments>]\n" exec cmd;
      fprintf o "where the queries are:\n";
      Query.describe o;)
    ~run:(fun config exec cmd -> function
    | [] -> None
    | name :: args -> Query.predefined config name args; Some ());

  define_command
    ~names:["bcl-to-fastq"; "b2f"]
    ~description:"Run, monitor, … the bcl_to_fastq function"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <command> <args>\n" exec cmd;
      fprintf o "Where the commands are:\n\
          \  * start: start a bcl-to-fastq function (try \"-help\").\n\
          \  * register-success <id>.\n\
          \  * register-failure <id> [<reason-log>].\n\
          \  * status <id> : Get the current status of an evaluation.\n\
          \  * fix-status <id> : Get the status and fix it if possible\n\
          \  * kill <id>.\n\
          \  * info <b2f-id or stats-dir>: Get stats about the result.\n")
    ~run:(fun config exec cmd ->
      let module B2F = Hitscore_b2f_commands in
      function
      | "start" :: args -> 
        B2F.start config (sprintf "%s <config> %s start" exec cmd) args
      | "register-success" :: id :: [] ->
        B2F.register_success config id
      | "register-failure" :: id  :: [] ->
        B2F.register_failure config id
      | "register-failure" :: id  :: reason :: [] ->
        B2F.register_failure ~reason config id
      | "status" :: id :: [] ->
        B2F.check_status config id
      | "fix-status" :: id :: [] ->
        B2F.check_status ~fix_it:true config id
      | "kill" :: id :: [] ->
        B2F.kill config id
      | "info" :: id :: [] ->
        B2F.info config id
      | _ -> None);
  
  define_command
    ~names:["wake-up"; "wu"]
    ~description:"Do some checks on the Layout"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s [-fix]\n" exec cmd)
    ~run:(fun config exec cmd -> function
    | [] -> Verify.wake_up config
    | ["-fix"] -> Verify.wake_up ~fix_it:true config
    | _ -> None);

  define_command 
    ~names:["get-hiseq-raw-info"; "ghri"]
    ~description:"Get information from an HiSeq Raw directory"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <hiseq-dir>\n" exec cmd)
    ~run:(fun config exec cmd -> function
    | [dir] -> Some (Hiseq_raw.get_info dir)
    | _ -> None);

  define_command ~names:["deliver"] ~description:"Deliver links to clients"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <bcl_to_fastq> <invoice> <dir> [<tag>]\n"
        exec cmd)
    ~run:(fun config exec cmd -> function
    | [bb; inv; dir] -> 
      Some (Prepare_delivery.run_function config bb inv dir None)
    | [bb; inv; dir; tag] -> 
      Some (Prepare_delivery.run_function config bb inv dir (Some tag))
    | _ -> None);

  define_command ~names:["delete-intensities";"di"]
    ~description:"Intensities Deletion commands"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <cmd>\n" exec cmd;
      fprintf o "  where <cmd> is:\n";
      fprintf o "    * register <id>: Register the manual deletion of \
        intensities in the hiseq-dir <id>.")
    ~run:(fun config exec cmd -> function
    | ["register"; dir] ->
      Some (Intensities_deletion.do_registration config dir)
    | _ -> None);

  define_command ~names:["register-hiseq-run"]
    ~description:"Register an HiSeq 2000 run"
    ~usage:(fun o exec cmd ->
      fprintf o "Usage: %s <profile> %s <date> <fcidA> <fcidB> [<note>]\n" exec cmd;
      fprintf o "  where fcid is a database id, a proper FCID, or '_'\n")
    ~run:(fun config exec cmd -> function
    | [date; fca; fcb] -> Some (Hiseq_run.register config date fca fcb None)
    | [date; fca; fcb; note] ->
      Some (Hiseq_run.register config date fca fcb (Some note))
    | _ -> None);

  let global_usage = function
    | `error -> 
      eprintf "ERROR: Main usage: %s <[config-file:]profile> <cmd> [OPTIONS | ARGS]\n" 
        Sys.argv.(0);
      eprintf "       try `%s help'\n" Sys.argv.(0);
    | `ok ->
      printf  "Main usage: %s <[config-file:]profile> <cmd> [OPTIONS | ARGS]\n"
        Sys.argv.(0);
  in
  match Array.to_list Sys.argv with
  | exec :: "-v" :: args
  | exec :: "-version" :: args
  | exec :: "--version" :: args
  | exec :: "version" :: args
  | exec :: _ :: "-v" :: args
  | exec :: _ :: "-version" :: args
  | exec :: _ :: "--version" :: args
  | exec :: _ :: "version" :: args ->
    printf "Hitscore v. %s\n" Hitscore_conf_values.version
  | exec :: "-h" :: args
  | exec :: "-help" :: args
  | exec :: "--help" :: args
  | exec :: "help" :: args
  | exec :: _ :: "-h" :: args
  | exec :: _ :: "-help" :: args
  | exec :: _ :: "--help" :: args
  | exec :: _ :: "help" :: args ->
    if args = [] then (
      global_usage `ok;
      printf "  where <cmd> is among:\n";
      printf "%s\n" (short_help "    ");
      printf "More Help: %s [<profile>] {-h,-help,--help,help} <cmd>\n" exec;
      printf "Also:  %s {-l,-list-config} [config-file]\n" exec
    ) else (
      List.iter args (fun cmd ->
        match find_command cmd with
        | Some (names, description, usage, run) ->
          usage stdout exec cmd
        | None ->
          printf "Unknown Command: %S !\n" cmd)
    )
  | exec :: "-l" :: args
  | exec :: "-list-config" :: args ->
    let config_file =
      match args with
      | [] -> Configuration_file.default () 
      | one :: [] -> one
      | _ -> eprintf "Too MANY arguments.\n"; global_usage `error; 
        failwith ""
    in
    let config = In_channel.(with_file config_file ~f:input_all) in
    let hitscore_config =
      match Hitscore_threaded.Configuration.(parse_str config) with
      | Ok o -> o
      | Error (`configuration_parsing_error e) ->
        eprintf "Error while parsing configuration: %s\n" (Exn.to_string e);
        failwith "STOP"
    in
    
    Configuration_file.print_all hitscore_config
   
    
  | exec :: profile :: cmd :: args ->
    let config_file, profile_name =
      match String.split profile ~on:':' with
      | [ one ] ->
        (Configuration_file.default (), one)
      | [ one; two ] ->
        (one, two)
      | _ -> failwithf "Can't understand: %s" profile ()
    in
    let config = In_channel.(with_file config_file ~f:input_all) in
    let hitscore_config =
      let open Result in
      Hitscore_threaded.Configuration.(
        parse_str config
        >>= fun c ->
        use_profile c profile_name)
      |! function
        | Ok o -> o
        | Error (`configuration_parsing_error e) ->
          eprintf "Error while parsing configuration: %s\n" (Exn.to_string e);
          failwith "STOP"
        | Error (`profile_not_found s) ->
          eprintf "Profile %S not found in config-file\n" s;
          failwith "STOP"
    in      
    begin match find_command cmd with
    | Some (names, description, usage, run) ->
      begin match run hitscore_config exec cmd args with
      | Some _ -> ()
      | None -> 
        eprintf "Wrong arguments!\n";
        usage stderr exec cmd;
      end
    | _ -> 
      eprintf "Unknown Command: %S !\n" cmd; 
      global_usage `error
    end

  | _ ->
    global_usage `error

