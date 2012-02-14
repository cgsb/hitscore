open Core.Std
let (|>) x f = f x

module Psql = Hitscoregen_psql
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
    Psql.verify db (eprintf "%s");
    eprintf "Done.\n";
    let open Out_channel in
    with_file (prefix ^ (Filename.basename file) ^ "_dsl_digraph.dot") 
      ~f:(fun o -> Psql.digraph db (output_string o));
      
    with_file (prefix ^ (Filename.basename file) ^ "_init.psql") 
      ~f:(fun o -> Psql.init_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_clear.psql") 
      ~f:(fun o -> Psql.clear_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_code.ml") 
      ~f:(fun o -> Hitscoregen_layout_ocaml.ocaml_code "" dsl (output_string o));

  | exec :: "codegen" :: in_file :: out_file :: [] ->
    let raw_dsl =
      In_channel.(with_file in_file ~f:(fun i -> (input_all i))) in
    Out_channel.(with_file out_file ~f:(fun o -> 
      Hitscoregen_layout_ocaml.ocaml_code 
        raw_dsl (parse_str raw_dsl) (output_string o)))
      
  | exec :: "codegen-itf" :: in_file :: out_file :: [] ->
    let dsl =
      In_channel.(with_file in_file ~f:(fun i -> parse_str (input_all i))) in
    Out_channel.(with_file out_file ~f:(fun o -> 
      Hitscoregen_layout_ocaml.ocaml_interface dsl (output_string o)))
      
  | exec :: "dbverify" :: file :: [] ->
    In_channel.(with_file file
                  ~f:(fun i -> Psql.verify
                    (input_all i |> Psql.parse_str) (eprintf "%s")))
  | exec :: "dbpostgres" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = Psql.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> Psql.init_db_postgres db (output_string o)));
        Out_channel.(with_file (Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> Psql.clear_db_postgres db (output_string o))))
  | exec :: "dbdraw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = Psql.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> Psql.digraph db (output_string o))))
  | exec :: "db_digraph" :: file :: outfile :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file outfile
                       ~f:(fun o -> Psql.digraph db (output_string o))))
  | exec :: "postgres" :: file :: outprefix :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file 
                       (outprefix ^ (Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> Psql.init_db_postgres db (output_string o)));
        Out_channel.(with_file 
                       (outprefix ^ Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> Psql.clear_db_postgres db (output_string o))))
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


