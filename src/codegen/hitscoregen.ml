open Core.Std
let (|>) x f = f x

module Psql = Hitscoregen_psql
open Hitscoregen_layout_dsl

let () =
  match Sys.argv |> Array.to_list with
      
  | exec :: "codegen-ocaml" :: in_file :: out_file :: [] ->
    let raw_dsl =
      In_channel.(with_file in_file ~f:(fun i -> (input_all i))) in
    Out_channel.(with_file out_file ~f:(fun o -> 
      Hitscoregen_layout_ocaml.ocaml_module 
        raw_dsl (parse_str raw_dsl) (output_string o)))
      
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


