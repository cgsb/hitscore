
open Core.Std
open Hitscore_app_util
open Hitscore
open Sequme_flow
open Sequme_flow_list
open Sequme_flow_sys


let commands = ref []

let define_command ~names ~description ~usage ~run =
  let meta_run a b c d =
    begin match Lwt_main.run (run a b c d) with
    | Ok () -> true
    | Error (`invalid_command_line s) ->
      eprintf "Wrong arguments: %s!\n" s;
      false
    | Error  e ->
      eprintf "HITSCORE: ERROR:\n  %s\n" (string_of_error e);
      true
    end
  in
  commands := (names, description, usage, meta_run) :: !commands

let short_help indent =
  String.concat ~sep:"\n"
    (List.map !commands
       (fun (names, desc, _, _) ->
         sprintf "%s * %s:\n%s%s%s" indent (String.concat ~sep:"|" names)
           indent indent desc))

let find_command cmd =
  List.find !commands (fun (names, _, _, _) -> List.exists names ((=) cmd))


module Configuration_file = struct

  let default () =
    sprintf "%s/.config/hitscore/config.sexp"
      (Option.value_exn ~message:"This environment has no $HOME !"
         (Sys.getenv "HOME"))

  let iter = List.iter

  let print_config config =
    let open Option in
    let open Configuration in
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
    List.iter (bcl_to_fastq_available_versions config) (fun version ->
      printf "Bcl-to-fastq %S\n" version;
      List.iter (bcl_to_fastq_pre_commands config ~version) (fun c ->
        printf "  PRE: %S\n" c;
      );
    );
    ()

  let print_env () =
    let open Option in
    iter (Sys.getenv "PGHOST") (printf "Env: PGHOST : %S\n");
    iter (Sys.getenv "PGPORT") (printf "Env: PGPORT : %S\n");
    iter (Sys.getenv "PGUSER") (printf "Env: PGUSER : %S\n");
    iter (Sys.getenv "PGDATABASE") (printf "Env: PGDATABASE : %S\n");
    iter (Sys.getenv "PGPASSWORD") (printf "Env: PGPASSWORD : %S\n");
    ()

  let print_all profiles =
    let open Configuration in
    List.iter (profile_names profiles) (fun n ->
      printf "** Configuration %S:\n" n;
      print_config (use_profile profiles n |! result_ok_exn ~fail:Not_found));
    printf "** Environment:\n";
    print_env ();
    ()

  let export_env config command =
    let open Option in
    let open Configuration in
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

  let () =
    define_command
      ~names:["print-configuration"; "pc"]
      ~description:"Display the current profile (and environment)."
      ~usage:(fun o exec cmd -> fprintf o "usage: %s <profile> %s\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [] ->
        printf "** Current configuration:\n";
        print_config config;
        printf "** Environment:\n";
        print_env ();
        return ()
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))));
    define_command
      ~names:["with-env"; "wenv"]
      ~description:"Run a command (default \"bash\") with the current \
                  profile's environment."
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s [<cmd>]\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [] -> export_env config "bash"; return ()
      | [cmd] -> export_env config cmd; return ()
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))));
    ()
end

module All_barcodes_sample_sheet = struct

  let illumina_barcodes = Assemble_sample_sheet.illumina_barcodes
  let bioo_barcodes = Assemble_sample_sheet.bioo_barcodes

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

  let () =
    define_command
      ~names:["all-barcodes-sample-sheet"; "abc"]
      ~description:"Make a sample sheet with all barcodes"
      ~usage:(fun o exec cmd ->
        fprintf o
          "usage: %s <profile> %s <flowcell id>  <list lanes/barcode vendors>\n"
          exec cmd;
        fprintf o "  example: %s %s D03NAKCXX N1 I2 I3 B4 B5 I6 I7 N8\n" exec cmd;
        fprintf o "  where N1 means no barcode on lane 1, I2 means all \
        Illumina barcodes on lane 2,\n  B4 means all BIOO barcodes on lane \
        4, etc.\n";)
      ~run:(fun config exec cmd -> function
      | flowcell :: specification ->
        make ~flowcell ~specification print_string;
        return ()
      | _ -> error (`invalid_command_line "expecting at least one argument"))

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
        System.command_to_string "groups" |!
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

  let () =
    define_command
      ~names:[ "pbs"; "make-pbs" ]
      ~description:"Generate PBS scripts"
      ~usage:(fun o exec cmd ->
        fprintf o
          "usage: %s <profile> %s [OPTIONS] <script-names>\nsee: %s %s -help\n"
          exec cmd exec cmd)
      ~run:(fun config exec cmd _ ->
        parse_cmdline (sprintf "%s %s" exec cmd) 1;
        return ())


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

  let () =
    define_command
      ~names:["gb2f"; "gen-bcl-to-fastq"]
      ~usage:(fun o exec bcl2fastq ->
        fprintf o  "usage: %s profile %s name basecalls-dir sample-sheet\n"
          exec bcl2fastq)
      ~description:"Prepare a BclToFastq run"
      ~run:(fun config exec cmd -> function
      | [ name; basecalls; sample_sheet ] ->
        prepare name basecalls sample_sheet;
        return ()
      | _ -> error (`invalid_command_line "expecting 3 arguments"))

end

module Backend_management = struct

  let to_file ?(verbose=true) configuration file =
    let log = if verbose then Some (eprintf "%s\n%!") else None in
    with_database ?log ~configuration (fun ~dbh ->
      Access.get_dump ~dbh
      >>= fun dump ->
      let dumps = Layout.sexp_of_dump dump |! Sexplib.Sexp.to_string_hum in
      Out_channel.(with_file file ~f:(fun o -> output_string o dumps));
      return ())

  let load_file ?(verbose=false) configuration file =
    let log = if verbose then Some (eprintf "%s\n%!") else None in
    with_database ?log ~configuration (fun ~dbh ->
      In_channel.with_file file ~f:(fun i ->
        Sexplib.Sexp.input_sexp i |! Layout.dump_of_sexp |!
            Access.insert_dump ~dbh))

  let () =
    define_command
      ~names:["dump-to-file"]
      ~description:"Dump the database to a S-Exp file"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s [-verbose] <filename>\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [file] -> to_file config file
      | ["-verbose"; file] -> to_file ~verbose:true config file
      | _ -> error (`invalid_command_line "no filename provided"));

    define_command
      ~names:["load-file"]
      ~description:"Load a dump the database (S-Exp file)"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s [-verbose] <filename>\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [file] -> load_file config file
      | ["-verbose"; file] -> load_file ~verbose:true config file
      | _ -> error (`invalid_command_line "no filename provided"));

    define_command
      ~names:["wipe-out-database"]
      ~description:"Empty the database (only Hitscore's tables)"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s <say-you're-sure>\n" exec cmd)
      ~run:(fun configuration exec cmd -> function
      | ["I'm sure"]
      | ["I am sure"]
      | ["I"; "am"; "sure"]
      | ["I-am-sure"]
      | ["i-am-sure"] ->
        with_database ~log:(eprintf "%s\n") ~configuration Backend.wipe_out
      | _ ->
        error (`invalid_command_line "please, explicitly say you're sure"));
    define_command
      ~names:["check-database"; "check-db"]
      ~description:"Check that the database is ready and fix it if it is not"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s\n" exec cmd)
      ~run:(fun configuration exec cmd -> function
      | [] ->
        with_database ~log:(eprintf "%s\n") ~configuration Backend.check_db
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))));
    ()


end

module Edit_database = struct

  let edit ~configuration db_id =
    with_database configuration (fun ~dbh ->
      Access.identify ~dbh db_id >>= fun p_initial ->
      Access.get_universal ~dbh p_initial >>= fun universal ->
      let sexp = Universal.sexp_of_value universal in
      let file = Filename.temp_file "hitscore_edition" ".scm" in
      write_file file ~content:(String.concat [
        sprintf ";; WARNING: you are editing the value of %S\n"
          (Universal.sexp_of_pointer p_initial |! Sexp.to_string_hum);
        sprintf ";; modifications to g_* fields are highly discouraged !!!\n";
        Sexp.to_string_hum sexp])
      >>= fun () ->
      ksprintf system_command "$EDITOR %s" file
      >>= fun () ->
      read_file file
      >>| String.rstrip
      >>= fun new_content ->
      let new_sexp = Sexp.of_string new_content |! Universal.value_of_sexp in
      Access.update_universal ~dbh new_sexp
      >>= fun () ->
      printf "Done.\n";

      return ()
    )


  let () =
    define_command ~names:["edit"]
      ~description:"Edit something in the database (with $EDITOR)"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s <db-id>" exec cmd)
      ~run:(fun configuration exec cmd -> function
      | [id] -> edit ~configuration Int.(of_string id)
      | _ ->
        error (`invalid_command_line "expecting one only argument: the ID"))
end

module Hiseq_raw = struct

  let fs_checks directory run_params =
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
        Unix.stat run_params |! Pervasives.ignore
      with
      | Unix.Unix_error _ ->
        eprintf "Cannot stat %s\n" run_params;
        failwith "Hiseq_raw.register"
    end;
    ()

  let register ?host config directory =
    if not (Filename.is_absolute directory) then (
      eprintf "%s is not an absolute path...\n" directory;
      failwith "Hiseq_raw.register"
    );
    let xml_run_params = Filename.concat directory "runParameters.xml" in
    fs_checks directory xml_run_params;

    let { Hitscore_interfaces.Hiseq_raw_information.
          flowcell_name     ;
          read_length_1     ;
          read_length_index ;
          read_length_2     ;
          with_intensities  ;
          run_date          ; } =
      let xml =
        In_channel.with_file xml_run_params ~f:(fun ic ->
          XML.(make_input (`Channel ic) |! in_tree)) in
      match Hiseq_raw.run_parameters (snd xml) with
      | Ok t -> t
      | Error (`parse_run_parameters (`wrong_date s)) ->
        failwithf "Error while parsing date in runParameters.xml: %s" s ()
      | Error (`parse_run_parameters (`wrong_field s)) ->
        failwithf "Error while parsing %s in runParameters.xml" s ()
    in
    let host =
      Option.value_exn
        ~error:(Error.of_exn (Failure "Cannot detect host!"))
        (match host with
        | Some s -> Some s
        | None -> System.detect_host ()) in
    (* Option.value_exn ~default:"bowery.es.its.nyu.edu" host in *)
    with_database config (fun ~dbh ->
      Access.Hiseq_raw.add_value ~dbh
        ~flowcell_name
        ~read_length_1:(Int.of_int_exn read_length_1)
        ?read_length_2:(Option.map read_length_2 Int.of_int_exn)
        ?read_length_index:(Option.map read_length_index Int.of_int_exn)
        ~with_intensities ~run_date ~host
        ~hiseq_dir_name:(Filename.basename directory)
      >>= fun in_db ->
      eprintf "The HiSeq raw directory was successfully added as %d\n"
        in_db.Layout.Record_hiseq_raw.id;
      return ())

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
          XML.(make_input (`Channel ic) |! in_tree)) in
      match Hiseq_raw.run_parameters (snd xml) with
      | Ok t -> t
      | Error (`parse_run_parameters (`wrong_date s)) ->
        failwithf "Error while parsing date in runParameters.xml: %s" s ()
      | Error (`parse_run_parameters (`wrong_field s)) ->
        failwithf "Error while parsing %s in runParameters.xml" s ()
    in
    let clustering =
      let xml =
        In_channel.with_file xml_read1 ~f:(fun ic ->
          XML.(make_input (`Channel ic) |! in_tree)) in
      match Hiseq_raw.clusters_summary (snd xml) with
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

  let () =
    define_command
      ~names:["register-hiseq-raw"; "rhr"]
      ~description:"Register a HiSeq raw directory"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s [-host <host-addr>] <absolute-path>\n"
          exec cmd;
        fprintf o "   (default host being %s)\n"
          (match System.detect_host () with
          | Some s -> sprintf "detected as %S" s
          | None -> sprintf "impossible to detect")
      )
      ~run:(fun config exec cmd -> function
      | [path] -> register config path
      | ["-host"; host; path] -> register config ~host path
      | _ -> error (`invalid_command_line "unexpected arguments"));
    define_command
      ~names:["get-hiseq-raw-info"; "ghri"]
      ~description:"Get information from an HiSeq Raw directory"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s <hiseq-dir>\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [dir] -> get_info dir; return ()
      | _ -> error (`invalid_command_line "unexpected arguments"));
    ()
end

module Verify = struct

  let check_directory dir =
    let m =
      wrap_io Lwt_unix.stat dir
      >>= fun {Lwt_unix. st_kind} ->
      begin if st_kind = Lwt_unix.S_DIR then
          return `ok
        else
          return (`not_a_directory st_kind)
      end
    in
    double_bind m ~ok:return
      ~error:(function
      | `io_exn (Unix.Unix_error (e,_,s)) ->
        return (`not_there (e, s))
      | `io_exn _ as e -> error e)

  let check_file_system ?(try_fix=false) configuration =
    let errors = ref [] in
    let add_error e = errors := e :: !errors; return () in
    with_database ~configuration (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#file_system#all
      >>= fun all_vols ->
      while_sequential all_vols (fun vol ->
        Common.path_of_volume ~dbh ~configuration vol#g_pointer
        >>= fun path ->
        check_directory path
        >>= fun checked ->
        begin match checked with
        | `ok -> return ()
        | `not_a_directory st_kind ->
          add_error (sprintf "%s is not a directory" path)
        | `not_there _ ->
          if try_fix then
            bind_on_error (
              ksprintf system_command "mkdir -p '%s'" path
              >>= fun () ->
              Access_rights.set_posix_acls ~dbh ~configuration (`dir path)
            ) (fun e ->
              add_error (sprintf "Cannot create %s: %s" path
                           (string_of_error e)))
          else
            add_error (sprintf "%s is missing" path)
        end
        >>= fun () ->
        Common.all_paths_of_volume ~dbh ~configuration vol#g_pointer
        >>= fun paths ->
        while_sequential paths (fun path ->
          check_directory path >>= fun checked ->
          begin match checked with
          | `ok -> return ()
          | `not_a_directory st_kind -> return ()
          | `not_there _ -> add_error (sprintf "%s is missing" path)
          end)
        >>= fun _ ->
        return ())
      >>= fun _ ->
      return ())
    >>= fun () ->
    return (List.rev !errors)

  let print_errors name l =
    List.iter l (fun s -> printf "%s: %s\n" name s);
    return ()

  let print_error_count name l =
    printf "%d %s\n" (List.length l) name;
    return ()

  let () =
    define_command
      ~names:["check-file-system"; "check-fs"]
      ~description:"Check the files registered in the database"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s [-quiet|-try-fix]\n" exec cmd;
        fprintf o "  -quiet : Non-verbose output (only count errors)\n";
        fprintf o "  -try-fix: Try to fix fixable errors\n"
      )
      ~run:(fun config exec cmd -> function
      | [] -> check_file_system config >>= print_errors "FS-Error"
      | [ "-quiet" ] -> check_file_system config >>= print_error_count "FS Error(s)"
      | [ "-try-fix" ] ->
        check_file_system ~try_fix:true config >>= print_errors "FS-Error"
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))

  let check_duplicates configuration =
    let errors = ref [] in
    let add_error e = errors := e :: !errors in
    let check l what =
      List.iter (List.sort ~cmp:compare l) (fun (name, project) ->
        let all = (List.filter l ~f:(fun (n, p) -> n = name && p <> project)) in
        match all with
        | []  -> ()
        | more ->
          ksprintf add_error "Duplicate %s: %S . %S %s" what
            (Option.value ~default:"" project) name
            (String.concat ~sep:""
               (List.map more (fun (n, p) ->
                 sprintf " ==  %S . %S" (Option.value ~default:"" p) n)));
      ) in
    with_database ~configuration (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#sample#all >>= fun all_samples ->
      while_sequential all_samples (fun sample ->
        return (sample#name, sample#project))
      >>= fun to_check ->
      check to_check "sample";
      layout#stock_library#all >>= fun all_stocks ->
      while_sequential all_stocks (fun stock ->
        return (stock#name, stock#project))
      >>= fun to_check ->
      check to_check "stock-library";
      return ())
    >>= fun () ->
    return !errors

  let check_function ~name ~status ~fail ~fix_it all =
    let all_started = List.filter ~f:(fun b -> b#g_status = `Started) all in
    printf "Function %s:" name;
    printf " %d started.\n" (List.length all_started);
    while_sequential all_started (fun f ->
      status f
      >>= fun status ->
      begin match status with
      | `running -> printf " * %d is running.\n" f#g_id; return ()
      | `started_but_not_running e ->
        printf " * %d is started but not running.\n" f#g_id;
        if fix_it then (
          printf "   -> FIXING\n";
          fail f ~reason:"checking_status_reported_started_but_not_running"
          >>= fun _ ->
          return ())
        else
          return ()
      | `not_started e ->
        printf "ERROR: The function %d is NOT STARTED: %S.\n" f#g_id
          (Layout.Enumeration_process_status.to_string e);
        return ()
      end)
    >>= fun _ ->
    return ()

  let verify_layout ?(verbose=false) configuration =
    let log = if verbose then Some (eprintf "%s\n%!") else None in
    with_database ?log ~configuration Verify_layout.all_pointers

  let () =
    define_command
      ~names:["verify-layout"]
      ~description:"Verify all the pointers in the Database"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s [-verbose]\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | [] -> verify_layout config
      | ["-verbose"] -> verify_layout ~verbose:true config
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))

  let wake_up ?(fix_it=false) ?(verbose=false) configuration =
    check_file_system configuration >>= fun fs_errors ->
    print_error_count "File-System Error(s)" fs_errors >>= fun () ->
    (if verbose then print_errors "FS-Error" fs_errors else return ())
    >>= fun () ->
    check_duplicates configuration >>= fun duplicate_errors ->
    print_error_count "Duplicates" duplicate_errors >>= fun () ->
    (if verbose then print_errors "Error" duplicate_errors else return ())
    >>= fun () ->
    with_database configuration (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#bcl_to_fastq#all >>= fun all_b2f ->
      check_function ~name:"bcl_to_fastq" all_b2f ~fix_it
        ~status:(fun f -> Bcl_to_fastq.status ~dbh ~configuration f#g_pointer)
        ~fail:(fun f ~reason -> Bcl_to_fastq.fail ~reason ~dbh f#g_pointer)
      >>= fun () ->
      layout#fastx_quality_stats#all >>= fun all_fxqs ->
      check_function ~name:"fastx_quality_stats" all_fxqs ~fix_it
        ~status:(fun f -> Fastx_quality_stats.status ~dbh ~configuration f#g_pointer)
        ~fail:(fun f ~reason -> Fastx_quality_stats.fail ~reason ~dbh f#g_pointer)
      >>= fun () ->
      return ())

  let () =
    define_command
      ~names:["wake-up"; "wu"]
      ~description:"Do some checks on the Layout"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s [-fix|-verbose]\n" exec cmd)
      ~run:(fun config exec cmd -> function
      | flags when List.for_all flags (fun f -> f = "-fix" || f = "-verbose") ->
        wake_up config
          ~fix_it:(List.exists flags ((=) "-fix"))
          ~verbose:(List.exists flags ((=) "-verbose"))
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))

end

module Dependencies = struct

  let show ~configuration ?(traversal="both") ?max_depth id =
    with_database configuration (fun ~dbh ->
      Access.identify ~dbh (Int.of_string id) >>= fun p_initial ->
      printf "%s\n" (Universal.sexp_of_pointer p_initial |! Sexp.to_string_hum);
      let rec show_deps how current_depth p =
        how ~dbh p >>= fun list_of_relatives ->
        while_sequential list_of_relatives (fun child ->
          printf "%s\\--> %s\n"
            String.(make (current_depth * 2) ' ')
            (Universal.sexp_of_pointer child |! Sexp.to_string_hum);
          if max_depth = Some current_depth then
            return ()
          else
            show_deps how (current_depth + 1) child)
        >>= fun _ ->
        return () in
      begin match traversal with
      | "both" ->
        printf "Down:\n";
        show_deps Dependency_graph.get_children 0 p_initial >>= fun () ->
        printf "Up:\n";
        show_deps Dependency_graph.get_parents 0 p_initial
      | "up" ->
        show_deps Dependency_graph.get_parents 0 p_initial
      | "down" ->
        show_deps Dependency_graph.get_children 0 p_initial
      | "follow-function" ->
        show_deps Dependency_graph.follow_function_once 0 p_initial
      | s ->
        failwithf "ERROR: can't understand %s" s  ()
      end
    )

  let () =
    define_command ~names:["dependencies"; "deps"]
      ~description:"Show the dependencies of a value/evaluation/volume"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s {up,down,both} [-max-depth <n>] \
                         <id>\n" exec cmd)
      ~run:(fun configuration exec cmd -> function
      | [ traversal; id ] ->
        show ~configuration ~traversal id
      | [ "-max-depth"; md; traversal; id ] ->
        show ~configuration ~traversal ~max_depth:Int.(of_string md) id
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))


end

module Flowcell = struct

  let check_lane_unused ~dbh lane =
    let layout = Classy.make dbh in
    layout#flowcell#all
    >>| List.filter ~f:(fun fc ->
      Array.exists fc#lanes ~f:(fun l -> l#id = lane))
    >>= fun q_res ->
    if List.length q_res <> 0 then (
      List.iter q_res (fun fc ->
        failwithf "Lane %d already used in flowcell %d (%S)"
          lane fc#g_id fc#serial_name ());
      return ())
    else
      return ()

  let checks_and_read_lengths ~dbh lanes =
    if List.length (List.dedup lanes) < List.length lanes then
      failwithf "Cannot reuse same lane" ();
    while_sequential lanes (fun id ->
      check_lane_unused ~dbh id >>= fun () ->
      Layout.Record_lane.(
        Access.Lane.get ~dbh (unsafe_cast id)
        >>= fun {g_value = { requested_read_length_1; requested_read_length_2; _ }} ->
        return (requested_read_length_1, requested_read_length_2 )
      ))
    >>= fun lanes_r1_r2 ->
    let first_r1, first_r2 =
      Option.value (List.hd lanes_r1_r2) ~default:(42, None) in
    let all_equal =
      List.for_all lanes_r1_r2
        ~f:(fun (r1, r2) -> r1 = first_r1 && r2 = first_r2) in
    if not all_equal then (
      printf "ERROR: Requested read-lengths are not all equal: [\n%s]\n"
        (List.mapi lanes_r1_r2 (fun i (r1, r2) ->
          match r2 with
          | None -> sprintf "  %d : SE %d\n" (i + 2) r1
          | Some r -> sprintf "  %d : PE %dx%d\n" (i + 2) r1 r)
          |! String.concat ~sep:"");
      failwith "ERROR"
    );
    return (first_r1, first_r2)

  let new_input_phix ~dbh r1 r2 =
    let layout = Classy.make dbh in
    layout#stock_library#all >>| List.filter ~f:(fun s -> s#name = "PhiX_v3")
    >>= fun phixs ->
    begin match phixs with
    |  [library] ->
      Access.Input_library.add_value ~dbh
        ~library:library#g_pointer ~submission_date:(Time.now ())
        ?volume_uL:None ?concentration_nM:None ~user_db:[| |] ?note:None
      >>= fun input_phix ->
      Common.add_log ~dbh (sprintf "(add_input_library_phix %d)"
                             input_phix.Layout.Record_input_library.id)
      >>= fun () ->
      let libraries = [| input_phix |] in
      Access.Lane.add_value ~dbh
        ~pool_name:"PhiX"
        ~libraries  ?total_volume:None ?seeding_concentration_pM:None
        ~pooled_percentages:[| 100. |]
        ~requested_read_length_1:r1 ?requested_read_length_2:r2
        ~contacts:[| |]
      >>= fun l_phix ->
      Common.add_log ~dbh (sprintf "(add_lane_phix %d)"
                             l_phix.Layout.Record_lane.id)
      >>= fun () ->
      return l_phix
    | _ ->
      failwithf "ERROR: Could not find PhiX_v3\n" ();
    end

  let new_empty_lane ~dbh r1 r2 =
    Access.Lane.add_value ~dbh
      ~libraries:[||] ?pool_name:None
      ?total_volume:None ?seeding_concentration_pM:None
      ~pooled_percentages:[| |]
      ~requested_read_length_1:r1 ?requested_read_length_2:r2
      ~contacts:[||]
    >>= fun lane ->
    Common.add_log ~dbh (sprintf "(add_empty_lane %d)" lane.Layout.Record_lane.id)
    >>= fun () ->
    return lane

  let parse_args =
    List.map ~f:(function
    | "PhiX" | "phix" | "PHIX" -> `phix
    | "Empty" | "empty" | "EMPTY" -> `empty
    | x ->
      let i = try Int.of_string x with e ->
          failwithf "Cannot understand arg: %S (should be an integer)" x () in
      `lane i)

  let make_lanes ~dbh lanes =
    checks_and_read_lengths ~dbh
      (List.filter_map lanes (function `lane i -> Some i | _ -> None))
    >>= fun (r1, r2) ->
    printf "It is a %S flowcell\n"
      (match r2 with
      | Some s -> sprintf "PE %dx%d" r1 s
      | None -> sprintf "SE %d" r1);
    while_sequential lanes ~f:(function
    | `phix ->  new_input_phix ~dbh r1 r2
    | `lane id -> return (Layout.Record_lane.unsafe_cast id)
    | `empty -> new_empty_lane ~dbh r1 r2)

  let register ~configuration ?(modify=false) name args =
    if List.length args <> 8 then
      failwith "Expecting 8 arguments after the flowcell name.";
    let lanes = parse_args args in
    with_database ~configuration (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#flowcell#all >>| List.filter ~f:(fun s -> s#serial_name = name)
      >>= function
      | [] ->
        printf "Registering %s\n" name;
        make_lanes ~dbh lanes
        >>= fun lanes ->
        Access.Flowcell.add_value ~dbh ~serial_name:name ~lanes:(Array.of_list lanes)
        >>= fun flowcell ->
        Common.add_log ~dbh (sprintf "(add_flowcell %d %s (lanes %s))"
                               flowcell.Layout.Record_flowcell.id name
                               (List.map lanes (fun i ->
                                 sprintf "%d" i.Layout.Record_lane.id)
                                 |! String.concat ~sep:" "))
      | [one] when not modify ->
        printf "ERROR: Flowcell name %S already used.\n" name;
        return ()
      | [one]  when modify ->
        printf "Modifying %s\n" name;
        make_lanes ~dbh lanes
        >>= fun lanes ->
        one#set_lanes (Array.of_list lanes)
        >>= fun () ->
        Common.add_log ~dbh (sprintf "(update_flowcell %d %s (lanes %s))"
                               one#g_id name
                               (List.map lanes (fun i ->
                                 sprintf "%d" i.Layout.Record_lane.id)
                                 |! String.concat ~sep:" "))
      | l ->
        printf "BIG-ERROR: Flowcell name %S already used %d times!!!\n"
          name (List.length l);
        return ()
    )

  let () =
    define_command
      ~names:["register-flowcell"]
      ~description:"Register a new flowcell (for existing lanes)"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s <flowcell-name> <L1> <L2> .. <L8>\n"
          exec cmd;
        fprintf o "where: Lx is 'PhiX', 'Empty', or an orphan lane's id.\n";
        fprintf o "and:\n  … %s -empty NAMECXX\n is equivalent to\n\
          \  … %s NAMECXX Empty Empty Empty Empty Empty Empty Empty Empty\n\
          " cmd cmd;
        fprintf o "and:\n  … %s -modify NAMECXX <L1> .. <L8>\n\
          will update the flowcell even if it already exists\n" cmd;
      )
      ~run:(fun configuration exec cmd -> function
      | [] -> error (`invalid_command_line "no arguments provided")
      | ["-empty"; name] ->
        register ~configuration name (List.init 8 (fun _ -> "Empty"))
      | "-modify" :: name :: lanes -> register ~configuration ~modify:true name lanes
      | name :: lanes -> register ~configuration name lanes
      )
end

module Query = struct

  let run_type r1 = function
    | Some r2 -> sprintf "PE %dx%d" r1 r2
    | None -> sprintf "SE %d" r1

  let predefined_queries = [
    ("orphan-pgm-pools",
     (["List of pgm-pools that are not referenced by any pgm-run."],
      fun _ dbh args ->
        let layout = Classy.make dbh in
        layout#pgm_pool#all >>= fun all_pools ->
        layout#pgm_run#all >>= fun all_runs ->
        let result =
          List.filter all_pools (fun pool ->
              List.for_all all_runs ~f:(fun run ->
                  not (Array.exists run#pool (fun p -> p#id = pool#g_id))))
        in
        while_sequential result (fun pool ->
            printf "PGM-pool: %d (name: %S) (invoices: %s) (contacts: %s)\n"
              pool#g_id (Option.value ~default:"" pool#pool_name)
              (Array.map pool#invoices ~f:(fun i -> Int.to_string i#id)
               |> String.concat_array ~sep:", ")
              (Array.map pool#contacts ~f:(fun c -> Int.to_string c#id)
               |> String.concat_array ~sep:", ");
            return ())
        >>= fun _ ->
        return ()));
    ("orphan-lanes",
     (["List of lanes which are not referenced by any flowcell."],
      fun _ dbh args ->
        let layout = Classy.make dbh in
        layout#flowcell#all >>= fun all_flowcells ->
        layout#lane#all >>= fun all_lanes ->
        layout#invoicing#all >>= fun all_invoicings ->
        layout#person#all >>= fun all_persons ->
        List.iter all_lanes (fun lane ->
          if List.exists all_flowcells ~f:(fun f ->
            Array.exists f#lanes ~f:(fun l -> l#id = lane#g_id)) then
            ()
          else (
            let invoicing =
              List.find_map all_invoicings ~f:(fun inv ->
                if Array.exists inv#lanes ~f:(fun l -> l#id = lane#g_id) then
                  List.find_map all_persons ~f:(fun p ->
                    if p#g_id = inv#pi#id then
                      Some (inv#g_id, p#g_id, p#family_name)
                    else
                      None)
                else
                  None) in
            printf "Lane %d (%s) is orphan%s, %S\n" lane#g_id
              (run_type lane#requested_read_length_1 lane#requested_read_length_2)
              Option.(
                value_map ~default:"" invoicing ~f:(fun (inv, pid, name) ->
                  sprintf " --> invoicing %d to %s (%d)" inv name pid))
              (Option.value ~default:"NO NAME" lane#pool_name)
          )
        );
        return ()));
    ("log",
     (["Display the log table.";
       "usage: log [<number of items>]"],
      fun _ dbh args ->
        let max_num =
          match args with
          | [n] -> Some (Int.of_string n)
          | _ -> None in
        let layout = Classy.make dbh in
        layout#log#all >>| List.sort ~cmp:(fun l1 l2 ->
          compare l2#g_last_modified l1#g_last_modified)
        >>= fun all_logs ->
        begin match max_num with
        | None -> return all_logs
        | Some s -> return (List.take all_logs s)
        end
        >>= fun logs ->
        List.iter logs (fun l ->
          printf "[%s] %s\n" (Time.to_string l#g_last_modified) l#log);
        return ()));

    ("orphan-input-libraries",
     (["List of libraries which are not referenced in any lane."],
      fun _ dbh args ->
        let layout = Classy.make dbh in
        layout#input_library#all >>= fun all_ils ->
        layout#lane#all >>= fun all_lanes ->
        let orphans =
          List.filter all_ils (fun il ->
            List.for_all all_lanes (fun lane ->
              Array.for_all lane#libraries (fun l -> l#id <> il#g_id))) in
        printf "%d orphan input-libraries\n" (List.length orphans);
        while_sequential orphans (fun oil ->
          oil#library#get
          >>= fun stock ->
          printf "  * %d submitted on %s (stock: %d, %s%s)\n"
            oil#g_id Time.(to_string oil#submission_date) stock#g_id
            Option.(value ~default:"" (map stock#project (sprintf "%s.")))
            stock#name;
          return ())
        >>= fun _ ->
        return ()));

    ("deliveries",
     (["List of potential deliveries for a flowcell"],
      fun configuration dbh args ->
        let fcid =
          match args with [one] -> one | _ -> failwith "expecting one argument" in
        let layout = Classy.make dbh in
        layout#flowcell#all >>| List.filter ~f:(fun f -> f#serial_name = fcid)
        >>= (function
        | [one] -> return one
        | _ -> failwithf "Not exactly one flowcell called %s" fcid ())
        >>= fun flowcell ->
        layout#invoicing#all >>= fun all_invoices ->
        let lanes = Array.to_list flowcell#lanes in
        let invoices =
          List.filter all_invoices ~f:(fun inv ->
            Array.exists inv#lanes ~f:(fun il ->
              List.exists lanes (fun fl -> il#id = fl#id))) in
        printf "%d invoices:\n" (List.length invoices);
        while_sequential invoices (fun inv ->
          inv#pi#get >>= fun pi ->
          printf " * %d to %s\n" inv#g_id pi#family_name;
          return ())
        >>= fun _ ->
        layout#bcl_to_fastq#all >>= fun b2fs ->
        while_sequential b2fs (fun b2f ->
          b2f#raw_data#get >>= fun hr ->
          if hr#flowcell_name = fcid then (
            match b2f#g_result with
            | None -> return None
            | Some u ->
              u#get >>= fun unaligned ->
              unaligned#directory#get >>= fun vol ->
              Common.path_of_volume ~dbh ~configuration vol#g_pointer >>= fun path ->
              return (Some (b2f, hr, unaligned, path))
          ) else
            return None)
        >>| List.filter_opt
        >>= fun interesting_b2fs ->
        printf "%d interesting bcl_to_fastq's\n" (List.length interesting_b2fs);
        List.iter interesting_b2fs (fun (b2f, hr, unaligned, path) ->
          printf " * %d %s%s, result: %d (%s)\n" b2f#g_id
            Option.(value ~default:"" (map b2f#tiles (sprintf "(tiles: %S) ")))
            Option.(value_map ~default:"" b2f#bases_mask ~f:(sprintf "(b-mask: %S) "))
            unaligned#g_id
            path
        );
        return ()));
  ]

  let describe out =
    List.iter predefined_queries ~f:(fun (name, (desc, _)) ->
      fprintf out " * %S:\n        %s\n" name
        (String.concat ~sep:"\n        " desc))

  let predefined hsc name args =
    with_database hsc (fun ~dbh ->
      begin match List.Assoc.find predefined_queries name with
      | Some (_, run) -> run hsc dbh args
      | None ->
        printf "Unknown custom query: %S\n" name;
        return ()
      end)
  let () =
    define_command
      ~names:["query"; "Q"]
      ~description:"Query the database with (predefined) queries"
      ~usage:(fun o exec cmd ->
        fprintf o "usage: %s <profile> %s <query> [<query arguments>]\n" exec cmd;
        fprintf o "where the queries are:\n";
        describe o;)
      ~run:(fun config exec cmd -> function
      | [] -> error (`invalid_command_line "no query argument")
      | name :: args -> predefined config name args)

end

module Prepare_delivery = struct

  let run_function configuration bb inv host dir directory_tag =
    if not (Filename.is_absolute dir)
    then (eprintf "%s is not an absolute path" dir; failwith "STOP");
    let host =
      Option.value_exn
        ~error:(Error.of_exn (Failure "Cannot detect host!"))
        (match host with
        | Some s -> Some s
        | None -> System.detect_host ()) in
    with_database ~configuration (fun ~dbh ->
      Unaligned_delivery.run ~dbh ~configuration ?directory_tag
        ~host
        ~bcl_to_fastq:(Layout.Function_bcl_to_fastq.unsafe_cast (Int.of_string bb))
        ~invoice:(Layout.Record_invoicing.unsafe_cast (Int.of_string inv))
        ~destination:dir
      >>= fun preparation ->
      eprintf "Done: Preparation: %d\n"
        preparation.Layout.Function_prepare_unaligned_delivery.id;
      return ())

  let do_repair configuration path =
    with_database configuration (fun ~dbh ->
      Unaligned_delivery.repair_path ~configuration ~dbh path)


  let () =
    define_command ~names:["deliver"] ~description:"Deliver links to clients"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s [-host <host>] <bcl_to_fastq> <invoice> <dir> [<tag>]\n"
          exec cmd;
        fprintf o "Usage: %s <profile> %s -redo <path>\n" exec cmd;
        fprintf o "Where the default <host> is %s\n"
          (match System.detect_host () with
          | Some s -> sprintf "detected as %S" s
          | None -> sprintf "impossible to detect");
      )
      ~run:(fun config exec cmd -> function
      | ["-redo"; path] -> do_repair config path
      | ["-host"; host; bb; inv; dir] -> run_function config bb inv (Some host) dir None
      | [bb; inv; dir] -> run_function config bb inv None dir None
      | [bb; inv; dir; tag] -> run_function config bb inv None dir (Some tag)
      | ["-host"; host; bb; inv; dir; tag] -> run_function config bb inv (Some host) dir (Some tag)
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))

end

module Intensities_deletion = struct
  let do_registration configuration dir =
    with_database configuration (fun ~dbh ->
      Delete_intensities.register ~dbh
        ~hiseq_raw: (Layout.Record_hiseq_raw.unsafe_cast (Int.of_string dir)))
    >>= fun di ->
    eprintf "Intensities deletion %d: DONE."
      di.Layout.Function_delete_intensities.id;
    return ()

  let () =
    define_command ~names:["delete-intensities";"di"]
      ~description:"Intensities Deletion commands"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s <cmd>\n" exec cmd;
        fprintf o "  where <cmd> is:\n";
        fprintf o "    * register <id>: Register the manual deletion of \
        intensities in the hiseq-dir <id>.")
      ~run:(fun config exec cmd -> function
      | ["register"; dir] -> do_registration config dir
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))
end

module Hiseq_run = struct

  let register configuration day_date fca fcb sequencer =
    with_database configuration (fun ~dbh ->
      let layout = Classy.make dbh in
      let flowcell id =
        if id = "_" then return None else
          try
            let p =
              Int.of_string id |! Layout.Record_flowcell.unsafe_cast in
            layout#flowcell#get p >>= fun f ->
            return (Some f#g_pointer)
          with e ->
            begin
              layout#flowcell#all >>| List.filter ~f:(fun f -> f#serial_name = id)
              >>= function
              | [one] -> return (Some one#g_pointer)
              | _ -> failwithf "Error while looking for flowcell %s" id ()
            end
      in
      flowcell fca >>= fun flowcell_a ->
      flowcell fcb >>= fun flowcell_b ->
      layout#add_hiseq_run ~date:(Time.of_string (day_date ^ " 10:00:00-04:00"))
        ~sequencer ?flowcell_a ?flowcell_b ?note:None ()
      >>= fun hsr ->
      printf "Added Hiseq run %d\n" hsr#id;
      layout#add_hiseq_statistics ~run:hsr#pointer ()
      >>= fun hs ->
      printf "Added Hiseq stats %d\n" hs#id;
      return ())

  let () =
    let default_sequencers = Configuration.default_sequencers in
    define_command ~names:["register-hiseq-run"]
      ~description:"Register an HiSeq 2000 run"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s <date> <fcidA> <fcidB> <sequencer>\n"
          exec cmd;
        fprintf o "  where fcid is a database id, a proper FCID, or '_'\n\
                  \  and sequencer is one of [%s]\n"
          (String.concat ~sep:", " default_sequencers))
      ~run:(fun config exec cmd -> function
      | [date; fca; fcb; sequencer] when List.mem default_sequencers sequencer ->
        register config date fca fcb sequencer
      | [date; fca; fcb; sequencer] ->
        error (`invalid_command_line (sprintf
                "don't know that sequencer: %s" sequencer))
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))))


end


module Pgm_run = struct

  let errf fmt = ksprintf (fun s -> error (`string s)) fmt

  let register ~configuration ?(force=false) ~run_name ~sequencer
      ~day_date ~run_type ~chip_type ?note pools =
    with_database configuration (fun ~dbh ->
        let layout = Classy.make dbh in
        begin try return (Time.of_string (day_date ^ " 10:00:00-04:00"))
        with e -> errf "Parsing date %S: %s" day_date (Exn.to_string e)
        end
        >>= fun date ->
        begin match run_type with
        | "200" | "400" -> return run_type
        | other when force -> return other
        | other ->
          errf "Run type error: %S should be 200, 400, or use the -force" other
        end
        >>= fun run_type ->
        begin match chip_type with
        | "314" | "316" | "318" -> return chip_type
        | other when force -> return other
        | other ->
          errf "Chip type error: %S should be 31[468], or use the -force" other
        end
        >>= fun chip_type ->
        layout#pgm_pool#all
        >>= fun all_pgm_pools ->
        let int s =
          try Int.of_string s |> return with e -> errf "not-an-int: %s" s in
        while_sequential pools (fun pool_id ->
            int pool_id
            >>= fun pid ->
            begin match List.find all_pgm_pools (fun p ->  p#g_id = pid) with
            | Some p -> return p#g_pointer
            | None -> errf "Cannot find pgm-pool %d" pid
            end)
        >>= fun pgm_pools ->
        layout#add_pgm_run () ~date ~run_name ~run_type ~chip_type ~sequencer
          ~pool:(Array.of_list pgm_pools) ?note
        >>= fun _ ->
        return ()
      )

  let () =
    let sequencer = "CGSB-PGM-1" in
    define_command ~names:["register-pgm-run"]
      ~description:"Register a PGM run"
      ~usage:(fun o exec cmd ->
          fprintf o "Usage: %s <profile> %s [-force] <run-name> <date> <run-type> \
                     <chip-type> <pool1> <pool2> ...\n"
            exec cmd;
        )
      ~run:(fun configuration exec cmd -> function
        | "-force" :: run_name :: day_date :: run_type :: chip_type :: pools ->
          register ~force:true ~configuration ~run_name ~sequencer
            ~day_date ~run_type ~chip_type pools
        | run_name :: day_date :: run_type :: chip_type :: pools ->
          register ~configuration ~sequencer ~day_date ~run_type ~chip_type pools
            ~run_name
        | l -> error (`invalid_command_line
                        (sprintf "don't know what to do with: %s"
                           String.(concat ~sep:", " l))))


end

module Fastx_qs = struct

  let start_parse_cmdline usage_prefix args =
    let (user, queue, nodes, ppn, wall_hours, hitscore_command, pbs_options) =
      pbs_related_command_line_options ~default_ppn:1 () in
    let from_b2f = ref None in
    let option_Q = ref None in
    let filter = ref [] in
    let options = pbs_options @ [
      ("-from-b2f", Arg.Int (fun i -> from_b2f := Some i),
       "<id>\n\tMake a link to the volume and luanch fastx on the \
               result of the bcl-to-fastq <id> ...");
      ("-option-Q", Arg.Int (fun q -> option_Q := Some q),
       "<q>\n\tUse <q> as the -Q option for fastx.");
      ("-filter", Arg.String (fun s -> filter := s :: !filter),
       "<filter>\n\t\
        Add <filter> to the filters used by find, one can add as\n\t\
        many filters as needed (understood as 'or'). If none are provided the \n\t\
        default is like -filter '*.fastq' -filter '*.fastq.gz'.");
    ] in
    let anon_args = ref [] in
    let anon s = anon_args := s :: !anon_args in
    let usage =
      sprintf "Usage: %s [OPTIONS] <search-flowcell-run>\n\
        \  Where   direc\n\
        \  Options:" usage_prefix in
    let cmdline = Array.of_list (usage_prefix :: args) in
    begin
      try Arg.parse_argv cmdline options anon usage;
          let filter_names = if !filter = [] then None else Some !filter in
          `go (List.rev !anon_args,
               !from_b2f,  !option_Q, filter_names,
               !user, !queue, !nodes, !ppn, !wall_hours, !hitscore_command)
      with
      | Arg.Bad b -> `bad b
      | Arg.Help h -> `help h
    end

  let find_a_link ~dbh b2fu_dir =
    let layout = Classy.make dbh in
    layout#generic_fastqs#all >>= fun all_gf ->
    while_sequential all_gf (fun gf ->
      gf#directory#get >>= fun d -> return (gf, d))
    >>= fun all_vols ->
    return (List.find all_vols ~f:(fun (gf, vol) ->
      let open Layout.File_system in
      match vol#g_content with
      | Link { id } -> id = b2fu_dir.Layout.File_system.id
      | _ -> false)
      |! Option.map ~f:fst)

  let call_fastx configuration volid opt_q path destdir =
    let work_m =
      with_database configuration (fun ~dbh ->
        Fastx_quality_stats.call_fastx ~dbh ~configuration
          ~volume:(Layout.File_system.unsafe_cast (Int.of_string volid))
          ~option_Q:(Int.of_string opt_q) path destdir)
    in
    bind_on_error work_m (fun e ->
      eprintf "ERROR(call-fastqx): %s" (string_of_error e);
      exit 4)

  let start configuration prefix cl_args =
    begin match (start_parse_cmdline prefix cl_args) with
    | `go (args, Some from_b2f, option_Q, filter_names,
           user, queue, nodes, ppn, wall_hours, hitscore_command) ->
      with_database configuration (fun ~dbh ->
        let layout = Classy.make dbh in
        layout#bcl_to_fastq#get_unsafe from_b2f
        >>= fun b2f ->
        begin match b2f#g_result with
        | None -> failwith "The bcl_to_fastq function has not succeeded"
        | Some p ->
          p#get >>= fun b2fu ->
          find_a_link ~dbh b2fu#directory#pointer
          >>= fun existsing_link ->
          begin match existsing_link with
          | None ->
            printf "Creating VFS link (coerce_b2f_unaligned)\n";
            Coerce_b2f_unaligned.run ~dbh ~configuration ~input:b2fu#g_pointer
            >>= fun f ->
            layout#coerce_b2f_unaligned#get f
            >>= fun ff ->
            (* Layout.Function_coerce_b2f_unaligned.(
                  get ~dbh f >>= fun ff -> *)
            return (Option.value_exn ff#g_result)#pointer
          | Some h ->
            printf "Found an existing Link: %d\n" h#g_id;
            return h#g_pointer
          end
        end
        >>= fun generic_fastqs ->
        Fastx_quality_stats.start ~dbh ~configuration
          generic_fastqs ?option_Q ?filter_names
          ?user ~nodes ~ppn ?queue ~wall_hours ~hitscore_command
        >>= fun pf ->
        printf "Evaluation %d started!\n%!"
          pf.Layout.Function_fastx_quality_stats.id;
        return ())
    | `go something_else ->
      failwith "non-from-b2f: not implemented"
    | `help h ->
      printf "%s" h; return ()
    | `bad b ->
      eprintf "%s" b; return ()
    end

  let register_success configuration id =
    with_database ~configuration (fun ~dbh ->
      let f = (Layout.Function_fastx_quality_stats.unsafe_cast id) in
      Fastx_quality_stats.succeed ~dbh ~configuration f
      >>= function
      | `success _ -> return ()
      | `failure (o, e) ->
        printf "The Function failed: %s\n" (string_of_error e);
        exit 3)

  let register_failure ?reason configuration id =
    with_database ~configuration (fun ~dbh ->
      let f = (Layout.Function_fastx_quality_stats.unsafe_cast id) in
      Fastx_quality_stats.fail ?reason ~dbh f
      >>= fun _ -> return ())

  let check_status ?(fix_it=false) configuration id =
    with_database ~configuration (fun ~dbh ->
      let f = (Layout.Function_fastx_quality_stats.unsafe_cast id) in
      Fastx_quality_stats.status ~dbh ~configuration f
      >>= function
      |  `running ->
        printf "The function is STILL RUNNING.\n";
        return ()
      | `started_but_not_running e ->
        printf "The function is STARTED BUT NOT RUNNING!!!\n";
        if fix_it then (
          printf "Fixing …\n";
          Fastx_quality_stats.fail ~dbh f
            ~reason:"checking_status_reported_started_but_not_running"
          >>= fun _ -> return ())
        else
          return ()
      | `not_started e ->
        printf "The function is NOT STARTED: %S.\n"
          (Layout.Enumeration_process_status.to_string e);
        return ())

  let kill configuration id =
    with_database ~configuration (fun ~dbh ->
      Fastx_quality_stats.kill ~dbh ~configuration
        (Layout.Function_fastx_quality_stats.unsafe_cast id)
    >>= fun _ -> return ())

  let () =
    define_command
      ~names:["fastx-quality-stats"; "fxqs"]
      ~description:"Run, monitor, … the fastx-quality-stats function"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s <command> <args>\n" exec cmd;
        fprintf o "Where the commands are:\n\
          \  * start: start the function (try \"-help\").\n\
          \  * register-success <id>.\n\
          \  * register-failure <id> [<reason-log>].\n\
          \  * status <id> : Get the current status of an evaluation.\n\
          \  * fix-status <id> : Get the status and fix it if possible\n\
          \  * kill <id>.\n")
      ~run:(fun config exec cmd ->
        function
        | "start" :: args ->
          start config (sprintf "%s <config> %s start" exec cmd) args
        | "register-success" :: id :: [] ->
          register_success config (Int.of_string id)
        | "register-failure" :: id  :: [] ->
          register_failure config (Int.of_string id)
        | "register-failure" :: id  :: reason :: [] ->
          register_failure ~reason config (Int.of_string id)
        | "status" :: id :: [] ->
          check_status config (Int.of_string id)
        | "fix-status" :: id :: [] ->
          check_status ~fix_it:true config (Int.of_string id)
        | "call-fastx" :: volid :: opt_q :: path :: destdir :: [] ->
          call_fastx config volid opt_q path destdir
        | "kill" :: id :: [] ->
          kill config (Int.of_string id)
        | l -> error (`invalid_command_line
                         (sprintf "don't know what to do with: %s"
                            String.(concat ~sep:", " l))))
end

module Users = struct

  let int s = try Some (Int.of_string s) with _ -> None

  let opt_val o msg =
    match o with None -> error (`string msg) | Some s -> return s

  let errf fmt = ksprintf (fun s -> error (`string s)) fmt

  let parse_lane_indexes s =
    begin match s with
    | "*" -> return [2;3;4;5;6;7;8]
    | other ->
      begin match String.split other ~on:',' with
      | [no_coma] ->
        begin match int no_coma with
        | Some i -> return [i]
        | None ->
          begin match String.split other ~on:'-' with
          | [b;t] ->
            opt_val (int b) (sprintf "%S should be an integer (parsing %s)" b s)
            >>= fun bot ->
            opt_val (int t) (sprintf "%S should be an integer (parsing %s)" t s)
            >>= fun top ->
            return (List.init (top - bot + 1) (fun i -> bot + i))
          | _ ->
            errf "Cannot parse %S" s
          end
        end
      | more_than_one ->
        while_sequential more_than_one (fun i ->
            opt_val (int i) (sprintf "%S should be an integer (parsing %s)" i s))
      end
    end


  let add_user_to_lanes ~configuration ~wet_run ~user ~lanes_specification =
    with_database ~configuration (fun ~dbh ->
        let layout = Classy.make dbh in
        layout#person#all
        >>| List.find ~f:(fun p ->
            Int.to_string p#g_id = user || p#login = Some user || p#email = user
            || Array.exists p#secondary_emails ~f:((=) user))
        >>= fun uopt ->
        opt_val uopt  (sprintf "cannot find user: %S" user)
        >>= fun person ->
        layout#flowcell#all >>= fun all_flowcells ->
        layout#lane#all >>= fun all_lanes ->
        begin
          while_sequential lanes_specification ~f:(fun spec ->
              begin match int spec  with
              | Some id ->
                let optlane = List.find all_lanes ~f:(fun l -> l#g_id = id) in
                opt_val optlane (sprintf "cannot find lane: %d" id)
                >>= fun l ->
                return [l]
              | None ->
                begin match String.split spec ~on:':' with
                | [fcid; lanes] ->
                  let fcopt =
                    List.find all_flowcells (fun f -> f#serial_name = fcid) in
                  opt_val fcopt (sprintf "Cannot find flowcell %s" fcid)
                  >>= fun fc ->
                  parse_lane_indexes lanes
                  >>= fun indexes ->
                  while_sequential indexes (fun i ->
                      try (fc#lanes).(i - 1)#get with _ ->
                        errf "%dth Lane does not exists in %s" i fcid)
                | s -> errf "cannot parse %S" spec
                end
              end)
          >>| List.concat
        end
        >>= fun lanes_to_treat ->
        while_sequential lanes_to_treat (fun l ->
            eprintf "Lane %d (people: %s)\n%!" l#g_id
              (String.concat ~sep:", "
                 (Array.to_list l#contacts
                  |> List.map ~f:(fun c -> Int.to_string c#id)));
            if wet_run then
              begin
                eprintf "[wet-run] Adding %d\n%!" person#g_id;
                let current =
                  Array.(map l#contacts ~f:(fun c -> c#pointer) |> to_list) in
                let to_set = List.dedup (current @ [person#g_pointer]) in
                l#set_contacts (Array.of_list to_set)
              end
            else
              begin
                eprintf "[dry-run] Would add %d\n%!" person#g_id;
                return ()
              end)
        >>= fun _ ->
        return ())

  let () =
    define_command
      ~names:["add-user-to-lanes"; "u2l"]
      ~description:"Add a user to a bunch of lanes"
      ~usage:(fun o exec cmd ->
        fprintf o "Usage: %s <profile> %s [-wet-run] <user> <spec>\n" exec cmd;
        fprintf o "Where the specification is a list of either:\n\
                   * lane database IDs\n\
                   * 'FCIDXX:*' → (lanes 2 to 8)\n\
                   * 'FCIDXX:n-m' → (lanes n to m)\n\
                   * 'FCIDXX:i,j,k' → (lanes i, j, k)\n\
                  ")
      ~run:(fun configuration exec cmd ->
        function
        | "-wet-run" :: user :: lanes_specification ->
          add_user_to_lanes ~wet_run:true ~configuration ~user ~lanes_specification
        | user :: lanes_specification ->
          add_user_to_lanes ~wet_run:false ~configuration ~user ~lanes_specification
        | [] -> error (`invalid_command_line "need at least a user name !"))


end

(* more define_command's: *)
let () =
  define_command
    ~names:["parse-submission-sheet"; "pss"]
    ~description:"Parse a submission sheet (CSV)"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s <profile> %s [-wet-run|-verbose] \
                <PhiX-config> <pss>\n" exec cmd;
      fprintf o "where PhiX-config is a coma-separated list of \
                 <pool-name>:<phix-percent> or 'No_PhiX'\n";
      fprintf o "example: %s dev %s -verbose A:1,C:4 pss_with_some_pools.csv\n"
        exec cmd)
    ~run:(fun config exec cmd -> fun l ->
      let dry_run = not (List.exists l ~f:((=) "-wet-run")) in
      let verbose = List.exists l ~f:((=) "-verbose") in
      let args = List.filter l ~f:(fun x -> x <> "-wet-run" && x <> "-verbose") in
      begin match args with
      | [phix_raw ; pss ] ->
        begin
          try
            let phix =
              List.filter_map (String.split ~on:',' phix_raw) ~f:(fun s ->
                match String.split s ~on:':' with
                | [ name; percent ] ->
                  Some ("Pool " ^ name, Int.of_string percent)
                | [one] when String.lowercase one = "no_phix" -> None
                | _ ->
                  eprintf "Can't understand %s\n" s;
                  failwith "phix parsing") in
            Hitscore_submission_sheet.parse ~dry_run ~verbose ~phix config pss
          with
          | e -> eprintf "Exception: %s\n" (Exn.to_string e); return ()
        end
      | l ->
        error (`invalid_command_line
                  (sprintf "don't know what to do with: %s"
                     String.(concat ~sep:", " l)))
      end);

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
          \  * kill <id>.\n")
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
      | l -> error (`invalid_command_line
                       (sprintf "don't know what to do with: %s"
                          String.(concat ~sep:", " l))));
  ()


(* MAIN *)
let () =
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
    let open Hitscore_conf_values in
    printf "Hitscore v. %s (built on or after %s)\n" version build_date
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
      match Configuration.(parse_str config) with
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
      Configuration.(
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
      begin match (run hitscore_config exec cmd args) with
      | true -> ()
      | false -> usage stderr exec cmd;
      end
    | _ ->
      eprintf "Unknown Command: %S !\n" cmd;
      global_usage `error
    end

  | _ ->
    global_usage `error
