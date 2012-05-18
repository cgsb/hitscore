open Core.Std
let (|>) x f = f x

open Hitscoregen_layout_dsl

let () =
  match Sys.argv |> Array.to_list with
      
  | exec :: "codegen-ocaml" :: in_file :: out_file :: [] ->
    let raw_dsl =
      In_channel.(with_file in_file ~f:(fun i -> (input_all i))) in
    Out_channel.(with_file out_file ~f:(fun o -> 
      Hitscoregen_layout_ocaml.ocaml_module 
        raw_dsl (parse_str raw_dsl) (output_string o)))
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


