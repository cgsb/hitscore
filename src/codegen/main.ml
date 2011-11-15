open Core.Std
let (|>) x f = f x

module DB_dsl = Hitscoregen_db_dsl
open Hitscoregen_layout_dsl

let () =
  match Sys.argv |> Array.to_list with
  | exec :: "all" :: file :: prefix :: [] ->
    let dsl =
      In_channel.(with_file file ~f:(fun i -> parse_str (input_all i))) in
    Out_channel.(
      with_file (prefix ^ (Filename.basename file) ^ "_digraph.dot") 
        ~f:(fun o -> digraph dsl (output_string o))
    );
    let db = to_db dsl in
    eprintf "Verification:\n";
    DB_dsl.verify db (eprintf "%s");
    eprintf "Done.\n";
    let open Out_channel in
    with_file (prefix ^ (Filename.basename file) ^ "_dsl_digraph.dot") 
      ~f:(fun o -> DB_dsl.digraph db (output_string o));
      
    with_file (prefix ^ (Filename.basename file) ^ "_init.psql") 
      ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_clear.psql") 
      ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_code.ml") 
      ~f:(fun o -> ocaml_code dsl (output_string o));

    with_file (prefix ^ (Filename.basename file) ^ "_fuzzdata_afew.psql")
      ~f:(fun o -> testing_inserts dsl 42 (output_string o));
    with_file (prefix ^ (Filename.basename file) ^ "_fuzzdata_alot.psql")
      ~f:(fun o -> testing_inserts dsl 4242 (output_string o));

  | exec :: "codegen" :: in_file :: out_file :: [] ->
    let dsl =
      In_channel.(with_file in_file ~f:(fun i -> parse_str (input_all i))) in
    Out_channel.(with_file out_file
                   ~f:(fun o -> ocaml_code dsl (output_string o)))
      
  | exec :: "dbverify" :: file :: [] ->
    In_channel.(with_file file
                  ~f:(fun i -> DB_dsl.verify
                    (input_all i |> DB_dsl.parse_str) (eprintf "%s")))
  | exec :: "dbpostgres" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = DB_dsl.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o)));
        Out_channel.(with_file (Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o))))
  | exec :: "dbdraw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = DB_dsl.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> DB_dsl.digraph db (output_string o))))
  | exec :: "db_digraph" :: file :: outfile :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file outfile
                       ~f:(fun o -> DB_dsl.digraph db (output_string o))))
  | exec :: "postgres" :: file :: outprefix :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file 
                       (outprefix ^ (Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o)));
        Out_channel.(with_file 
                       (outprefix ^ Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o))))
  | exec :: "digraph" :: file :: outfile :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let dsl = parse_str (In_channel.input_all i) in
        Out_channel.(with_file outfile
                       ~f:(fun o -> digraph dsl (output_string o))))
  | _ ->
    eprintf "usage: \n\
       sexp2db {dbverify, dbpostgres, dbdraw, ...} <sexp_file>\n\
       sexp2db all <sexp_file> <outprefix>\n";
    ()


