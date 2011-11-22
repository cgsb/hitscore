
open Core.Std
let (|>) x f = f x

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
        "<path>\n\tAdd an environment variable.");
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

    List.iter ~f:(fun name ->
      let out_dir_name = sprintf "%s/%s" !root name in
      System.command_exn (sprintf "mkdir -p %s" out_dir_name); 
      let script = 
        sprintf "#!/bin/bash

#PBS -m abe
#PBS -M %s
#PBS -l %swalltime=12:00:00
#PBS -V
#PBS -o %s/%s.stdout
#PBS -e %s/%s.stderr
#PBS -N %s
%s

export NAME=%s
export OUT_DIR=%s/
%s
echo \"Script $NAME Starts on `date -R`\"

%s

echo \"Script $NAME Ends on `date -R`\"

" !email
          (match !nodes, !ppn with 0, 0 -> ""
          | n, m -> sprintf "nodes=%d:ppn=%d," n m)
          out_dir_name name
          out_dir_name name
          name
          (match !queue with None -> "" | Some s -> sprintf "#PBS -q %s\n" s)
          name
          out_dir_name
          (String.concat ~sep:"\n" (List.map ~f:(sprintf "export %s") !variables))
          (Option.map ~f:In_channel.(with_file ~f:input_lines) !template |> 
              Option.value ~default:[] |> 
                  List.fold ~f:(fun a b -> sprintf "%s\n%s" a b)
                            ~init:"# User script:")
      in
      Out_channel.with_file (sprintf "%s/script_%s.pbs" out_dir_name name) 
        ~f:(fun o -> fprintf o "%s" script);
    ) !names;
    
    ()




end



let () =
  let usage = function
    | `error -> 
      eprintf "ERROR: usage: %s <cmd> [OPTIONS | ARGS]\n" Sys.argv.(0);
      eprintf "       try `%s help'\n" Sys.argv.(0);
    | `ok ->
      printf  "usage: %s <cmd> [OPTIONS | ARGS]\n" Sys.argv.(0)
  in
  match Array.to_list Sys.argv with
  | exec :: "-h" :: _
  | exec :: "-help" :: _
  | exec :: "--help" :: _
  | exec :: "help" :: _ ->
    usage `ok;
    printf "  where <cmd> is among:\n";
    printf "    * all-barcodes-sample-sheet | abc : \
                  Make sample sheets on stdout\n";
    printf "    * make-pbs | pbs : generate PBS scripts\n";
    printf "  please try `%s <cmd> -help'\n" exec;
  | exec :: "all-barcodes-sample-sheet" :: args
  | exec :: "abc" :: args ->
    begin match args with
    | "-h" :: _
    | "-help" :: _
    | "--help" :: _ ->
      printf "usage: %s abc <flowcell id>  <list lanes/barcode vendors>\n" exec;
      printf "  example: %s abc D03NAKCXX N1 I2 I3 B4 B5 I6 I7 N8\n" exec;
      printf "  where N1 means no barcode on lane 1, I2 means all \
        Illumina barcodes on lane 2,\n  B4 means all BIOO barcodes on lane \
        4, etc.\n";
    | flowcell :: specification ->
      All_barcodes_sample_sheet.make ~flowcell ~specification print_string
    | _ -> usage `error
    end

  | exec :: pbs :: args when pbs = "pbs" || pbs = "make-pbs" ->

    PBS_script_generator.parse_cmdline (sprintf "%s %s" exec pbs) 1

 | _ ->
   usage `error

