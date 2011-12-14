
open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
module Hitscore_db = Hitscore_threaded.Layout

module System = struct

  let command_exn s = 
    let status = Unix.system s in
    if not (Unix.Process_status.is_ok status) then
      ksprintf failwith "System.command_exn: %s" 
        (Unix.Process_status.to_string_hum status)
    else
      ()

  let command_to_string s =
    Unix.open_process_in s |> In_channel.input_all
        
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

  let to_file file =
    let hsc = Hitscore_threaded.configure () in
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let dump =
        match Hitscore_db.get_dump ~dbh with
        | Ok dump ->
          Hitscore_db.sexp_of_dump dump |> Sexplib.Sexp.to_string_hum
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
      
  let load_file file =
    let hsc = Hitscore_threaded.configure () in
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let insert_result = 
        In_channel.with_file file ~f:(fun i ->
          Sexplib.Sexp.input_sexp i |> Hitscore_db.dump_of_sexp |>
              Hitscore_db.insert_dump ~dbh) in
      begin match insert_result with 
      | Ok () -> eprintf "Load: Ok\n%!"
      | Error `wrong_version ->
        eprintf "Load: Wrong Version Error\n%!"
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

  module XML = struct
    include Xmlm
    type tree = E of tag * tree list | D of string
    let in_tree i = 
      let el tag childs = E (tag, childs)  in
      let data d = D d in
      input_doc_tree ~el ~data i
  end
  let register ?host directory =
    if not (Filename.is_absolute directory) then (
      eprintf "%s is not an absolute path...\n" directory;
      failwith "Hiseq_raw.register"
    );
    begin try 
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
    let xml_file = Filename.concat directory "runParameters.xml" in
    begin try Unix.stat xml_file |> ignore
      with 
      | Unix.Unix_error _ ->
        eprintf "Cannot stat %s\n" xml_file;
        failwith "Hiseq_raw.register"
    end;
    let read1 = ref None in
    let read2 = ref None in
    let idx_read = ref None in
    let intensities_kept = ref None in
    let flowcell = ref None in
    let start_date = ref None in
    In_channel.with_file xml_file ~f:(fun ic ->
      let xml = XML.(make_input (`Channel ic) |> in_tree) in
      let rec go_through = function
        | XML.E (((_,"Read1"), _), [ XML.D i ]) ->
          read1 := Some (Int32.of_string i) 
        | XML.E (((_,"Read2"), _), [ XML.D i ]) ->
          read2 := Some (Int32.of_string i) 
        | XML.E (((_,"KeepIntensityFiles"), _), [ XML.D b ]) ->
          intensities_kept := Some (Bool.of_string b)
        | XML.E (((_,"IndexRead"), _), [ XML.D i ]) ->
          idx_read := Some (Int32.of_string i)
        | XML.E (((_,"Barcode"), _), [ XML.D s ]) ->
          flowcell := Some s
        | XML.E (((_,"RunStartDate"), _), [ XML.D s ]) ->
          let scanned =
            Scanf.sscanf "110630" "%2d%2d%2d"
              (sprintf "20%d-%02d-%02d 09:00:00.000000-05:00") in
          start_date := Some (Time.of_string scanned)
        | XML.E (t, tl) -> List.iter tl go_through
        | XML.D s -> ()
      in
      go_through (snd xml)
    );
    let flowcell_name = 
      match !flowcell with
      | None ->
        eprintf "Could not read the flowcell id from the XML file\n";
        failwith "Hiseq_raw.register"
      | Some s -> s in
    let read_length_1 =
      match !read1 with
      | None ->
        eprintf "Could not read the read_length_1 from the XML file\n";
        failwith "Hiseq_raw.register"
      | Some s -> s in
    if Option.is_none !read2 then
      eprintf "Warning: This looks like a single-end run\n";
    if Option.is_none !idx_read then
      eprintf "Warning: This looks like a non-indexed run\n";
    let with_intensities =
      match !intensities_kept with
      | None ->
        eprintf "Could not find if the intensities were kept or not\n";
        failwith "Hiseq_raw.register"
      | Some s-> s in
    let run_date =
      match !start_date with
      | None ->
        eprintf "Could not find if the start date\n";
        failwith "Hiseq_raw.register"
      | Some s-> s in
    let host = Option.value ~default:"bowery.es.its.nyu.edu" host in 
    let hsc = Hitscore_threaded.configure () in
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let hs_raw =
        Hitscore_threaded.Layout.Record_hiseq_raw.add_value ~dbh
          ~flowcell_name
          ~read_length_1 ?read_length_2:!read2 ?read_length_index:!idx_read
          ~with_intensities ~run_date ~host
          ~hiseq_dir_name:directory
      in
      begin match hs_raw with
      | Ok in_db ->
        Hitscore_threaded.db_disconnect hsc dbh  |> ignore;
        eprintf "The HiSeq raw directory was successfully added as %ld\n"
        in_db.Hitscore_threaded.Layout.Record_hiseq_raw.id
      | Error (`layout_inconsistency (`record_hiseq_raw,
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


end

module Verify = struct


  let check_file_system ?(verbose=true) root =
    let hsc = Hitscore_threaded.configure () in
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      let (>>>) x f = Result.ok_exn ~fail:(Failure f) x in
      let vols = Hitscore_db.File_system.get_all ~dbh >>> "File_system.get_all" in
      List.iter vols ~f:(fun vol -> 
        Hitscore_db.File_system.cache_volume ~dbh vol >>> "Caching volume" |>
            (fun volume ->
              let path =
                Filename.concat root 
                  Hitscore_db.File_system.(volume |> volume_entry_cache |> 
                      volume_entry |> entry_unix_path) in
              if verbose then
                eprintf "* Checking volume %S\n" path;
              Unix.(try
                      let vol_stat = stat path in
                      if vol_stat.st_kind <> S_DIR then
                        eprintf "ERROR(volume): %S:\n  Not a directory\n" path
                      else
                        if verbose then
                          eprintf "-> OK\n"
                        else
                          ()
                with
                | Unix_error (e, _, s) ->
                  eprintf "ERROR(volume): %S:\n %S (%S)\n" path (error_message e) s);
              begin match Hitscore_db.File_system.volume_trees volume with
              | Error (`cannot_recognize_file_type s) ->
                eprintf "ERROR(get-files): %S:\n  cannot_recognize_file_type %S??\n"
                  path s
              | Error (`inconsistency_inode_not_found i) ->
                eprintf "ERROR(get-files): %S:\n  inconsistency_inode_not_found %ld"
                  path i
              | Ok trees ->
                let paths =
                  Hitscore_db.File_system.trees_to_unix_paths trees in
                List.iter paths ~f:(fun s -> 
                  let filename = Filename.concat path s in
                  if verbose then eprintf "  \\-> %S\n" filename;
                  Unix.(try
                      let file_stat = stat filename in
                      ignore file_stat
                    with
                    | Unix_error (e, _, s) ->
                      eprintf "ERROR(file): %S:\n %S (%S)\n"
                        filename (error_message e) s);
                );
              end))
    | Error (`pg_exn e) ->
      eprintf "Could not connect to the database: %s\n" (Exn.to_string e)


end

module FS = struct

  let add_files_to_volume root vol files =
    let open Hitscore_threaded.Result_IO in
    let hsc = Hitscore_threaded.configure ~root_directory:root () in
    match Hitscore_threaded.db_connect hsc with
    | Ok dbh ->
      Hitscore_threaded.Layout.File_system.(
        let vol_cache = cache_volume ~dbh { id = vol } in
        begin match vol_cache >>| volume_entry_cache
                     >>| volume_entry >>| entry_unix_path with
        | Ok path ->
          let file_args = String.concat ~sep:" " files in
          eprintf "Copying %s to %s/%s\n" file_args root path;
          ksprintf System.command_exn "cp %s %s/%s/" file_args root path
        | Error (`layout_inconsistency (`file_system,
                                        `select_did_not_return_one_cache (s, i))) ->
          eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.cache_volume detected an inconsistency\n\
          FILE_SYSTEM: select_did_not_return_one_cache (%s, %d)" s i;
          failwith "STOP"
        | Error (`pg_exn e) ->
          eprintf "ERROR(FS.add_tree_to_volume): \n\
          Layout.File_system.cache_volume had a PGOCaml error:\n%s"
            (Exn.to_string e);
          failwith "STOP"
        end;
        eprintf "Copy: DONE (won't go back).\n";
        begin match
            add_tree_to_volume ~dbh { id = vol } 
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


end



let commands = ref []

let define_command ~names ~description ~usage ~run =
  commands := (names, description, usage, run) :: !commands

let short_help indent =
  String.concat ~sep:"\n"
    (List.map !commands
       (fun (names, desc, _, _) ->
         sprintf "%s * %s : %s" indent (String.concat ~sep:"|" names) desc))

let find_command cmd = 
  List.find !commands (fun (names, _, _, _) -> List.mem cmd ~set:names)

let () =
  define_command 
    ~names:["all-barcodes-sample-sheet"; "abc"]
    ~description:"Make a sample sheet with all barcodes"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s %s <flowcell id>  <list lanes/barcode vendors>\n" 
        exec cmd;
      fprintf o "  example: %s %s D03NAKCXX N1 I2 I3 B4 B5 I6 I7 N8\n" exec cmd;
      fprintf o "  where N1 means no barcode on lane 1, I2 means all \
        Illumina barcodes on lane 2,\n  B4 means all BIOO barcodes on lane \
        4, etc.\n";)
    ~run:(fun exec cmd -> function
      | flowcell :: specification ->
        Some (All_barcodes_sample_sheet.make ~flowcell ~specification print_string) 
      | _ -> None);
  
  define_command
    ~names:[ "pbs"; "make-pbs" ]
    ~description:"Generate PBS scripts"
    ~usage:(fun o exec cmd -> 
      fprintf o "usage: %s %s [OPTIONS] <script-names>\nsee: %s %s -help\n"
        exec cmd exec cmd)
    ~run:(fun exec cmd _ ->
      Some (PBS_script_generator.parse_cmdline (sprintf "%s %s" exec cmd) 1));

  define_command
    ~names:["gb2f"; "gen-bcl-to-fastq"]
    ~usage:(fun o exec bcl2fastq ->
      fprintf o  "usage: %s %s name basecalls-dir sample-sheet\n" exec bcl2fastq)
    ~description:"Prepare a BclToFastq run"
    ~run:(fun exec cmd -> function
      | [ name; basecalls; sample_sheet ] ->
        Some (Gen_BclToFastq.prepare name basecalls sample_sheet)
      | _ -> None);

  define_command
    ~names:["dump-to-file"]
    ~description:"Dump the database to a S-Exp file"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s %s <filename>\n" exec cmd)
    ~run:(fun exec cmd -> function
      | [file] -> Some (Dumps.to_file file)
      | _ -> None);

  define_command
    ~names:["load-file"]
    ~description:"Load a dump the database (S-Exp file)"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s %s <filename>\n" exec cmd)
    ~run:(fun exec cmd -> function
      | [file] -> Some (Dumps.load_file file)
      | _ -> None);

  define_command 
    ~names:["register-hiseq-raw"; "rhr"]
    ~description:"Register a HiSeq raw directory"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s %s [-host <host-addr>] <absolute-path>\n" exec cmd;
      fprintf o "   (default host being bowery.es.its.nyu.edu)\n")
    ~run:(fun exec cmd -> function
      | [path] -> Some (Hiseq_raw.register path)
      | ["-host"; host; path] -> Some (Hiseq_raw.register ~host path)
      | _ -> None);
 

  define_command
    ~names:["check-file-system"; "check-fs"]
    ~description:"Check the files registered in the database"
    ~usage:(fun o exec cmd ->
      fprintf o "usage: %s %s [-quiet] <root-dir>\n" exec cmd;
      fprintf o "  -quiet : Non-verbose output\n"
    )
    ~run:(fun exec cmd -> function
      | [ path ] -> Some (Verify.check_file_system path)
      | [ "-quiet"; path ] -> Some (Verify.check_file_system ~verbose:false path)
      | _ -> None);

  define_command
    ~names:["add-files-to-volume"; "afv"]
    ~description:"Move files to a volume (at its root)"
    ~usage:(fun o exec cmd ->
      fprintf o "%s %s <root> <volume_id : int> <file1> <file2> ...\n" exec cmd)
    ~run:(fun exec cmd -> function
      | root :: vol :: files ->
        begin 
          try 
            let v = Int32.of_string vol in
            Some (FS.add_files_to_volume root v files)
          with e -> None
        end
      | _ -> None);

  let global_usage = function
    | `error -> 
      eprintf "ERROR: usage: %s <cmd> [OPTIONS | ARGS]\n" Sys.argv.(0);
      eprintf "       try `%s help'\n" Sys.argv.(0);
    | `ok ->
      printf  "usage: %s <cmd> [OPTIONS | ARGS]\n" Sys.argv.(0)
  in
  match Array.to_list Sys.argv with
  | exec :: "-h" :: args
  | exec :: "-help" :: args
  | exec :: "--help" :: args
  | exec :: "help" :: args ->
    if args = [] then (
      global_usage `ok;
      printf "  where <cmd> is among:\n";
      printf "%s\n" (short_help "    ");
      printf "  please try `%s -help <cmd>\n" exec;
    ) else (
      List.iter args (fun cmd ->
        match find_command cmd with
        | Some (names, description, usage, run) ->
          usage stdout exec cmd
        | None ->
          printf "Unknown Command: %S !\n" cmd)
    )

  | exec :: cmd :: args ->
    begin match find_command cmd with
    | Some (names, description, usage, run) ->
      begin match run exec cmd args with
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

