
open Core.Std

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
    printf "    * all_barcodes_sample_sheet | abc: Make sample sheets on stdout\n";
    printf "  please try `%s <cmd> -help'\n" exec;
  | exec :: "all_barcodes_sample_sheet" :: args
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

 | _ ->
   usage `error

