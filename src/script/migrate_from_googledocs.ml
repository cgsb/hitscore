#! /usr/bin/env ocamlscript
Ocaml.ocamlflags := ["-thread"];
Ocaml.packs := ["hitscore"; "csv"; "pcre"]
--

open Core.Std
let (|>) x f = f x

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


let prog = Filename.basename Sys.argv.(0)
let usage = sprintf
"usage: %s metdata_dir

  Migrate data from google doc spreadsheet \"CGSB Genomics Core
  Metadata\" to new PostgreSQL database. This migration was done in
  Nov 2011.

  metadata_dir - Directory to which google spreadsheet has been
  exported in tab-delimited format. There should be one file per sheet
  in the overall spreadsheet. See code for additional details on
  expected format.

  Environment Variables: PostgreSQL environment variables should be
  set as necessary for PG'OCaml to connect to the appropriate
  database. The database should also be initialized with appropriate
  tables but all tables should be empty.
" prog

module Usual_thread = struct
  include PGOCaml.Simple_thread

  let err = eprintf "%s%!"
  let map_s f l = List.map ~f l

end      
module Hitscore_db = Hitscore_db_access.Make(Usual_thread)
module PGOCaml = Hitscore_db.PGOCaml


let pcre_matches rex str = 
  try ignore (Pcre.exec ~rex str); true with Not_found -> false


let main metadata_prefix dbh =
  
  (* let mkpath = List.fold_left ~f:Filename.concat ~init:metadata_dir in *)
  let load = Csv.load ~separator:',' in
  let load_table t =
    load (sprintf "%s%s.csv" metadata_prefix t) |> Csv.square |> List.tl_exn in
  let persons =
    let note = sprintf "(From_migration %S)" Time.(to_string (now ())) in
    let nyu_login_email = Pcre.regexp "[a-z]+[0-9]+@nyu.edu" in
    let tbl = load_table "Person" in
    List.map tbl ~f:(function
      | [first;surname;email;nickname] ->
        let nickname = if nickname = "" then None else Some nickname in
        let login =
          if pcre_matches nyu_login_email email then
            List.hd (String.split ~on:'@' email)
          else
            begin match surname with
            | "Agarwal" -> Some "aa144"
            | "Mondet" -> Some "sm4431"
            | _ -> None
            end 
        in
        let id  : Hitscore_db.Record_person.t = 
          Hitscore_db.Record_person.add_value
            ~print_name:(sprintf "%s %s" first surname)
            ~family_name:surname ~note ?login
            ~email ?nickname ~dbh in
        (id, surname)
      | _ -> failwith "persons") in

  (persons) |> ignore

let () =
  match Array.to_list Sys.argv with
  | exec :: dir :: [] -> 
    let dbh = PGOCaml.connect() in
    main dir dbh;
    let run_psql s =
      sprintf "echo '<h3>%s</h3>' >> ttt.html\n\
               psql --html -c '%s' >> ttt.html\n" s s
    in
    let report l = 
      Unix.system 
        (sprintf "rm ttt.html\n\
                 %s\n" (String.concat ~sep:"\n" (List.map l run_psql))) |> ignore in
    let all = sprintf "select * from %s;" in
    report [
      all "person";
    ]
  | _ -> ()
