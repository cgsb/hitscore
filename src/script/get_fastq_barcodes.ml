#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "batteries"; "sequme"];;
--

open Biocaml_std
module Fastq = Biocaml_fastq
module HT = struct
  include BatHashtbl
  include BatHashtbl.Labels
  include BatHashtbl.Exceptionless
end

let () =

  let results = HT.create 42 in

  Array.iter (Array.sub Sys.argv 1 (Array.length Sys.argv - 1)) ~f:(fun line ->
    eprintf "Play with %s\n%!" line;
    let i = BatUnix.open_process_in (sprintf "gunzip -c %s" line) in
    let fastq = Fastq.enum_input i in
    
    let rec parse fastq =
      match Enum.get fastq with
      | Some (title, sequence, another_title, quality_score) ->
        let { Sequme_illumina.Fastq.index; _ } =
          Sequme_illumina.Fastq.sequence_id_of_string title in
        begin match HT.find results index with
        | Some count ->
          HT.replace results index (count + 1)
        | None -> 
          HT.add results index 1
        end;
        parse fastq
      | None -> ()
    in
    parse fastq;
    IO.close_in i;
  );
  
  let ns = HT.create 6 in
  List.iter (results |> HT.enum |> List.of_enum |> List.fast_sort)
    ~f:(fun (index, count) ->
      let number_of_Ns = 
        String.fold_left 
          (fun x c -> x + (if c = 'N' then 1 else 0)) 0 index in
      begin match HT.find ns number_of_Ns with
      | Some c ->
        HT.replace ns number_of_Ns (c + count)
      | None -> 
        HT.add ns number_of_Ns count
      end;
      printf "Index %-20s : %d\n%!" index count;
    );
  List.iter (ns |> HT.enum |> List.of_enum |> List.fast_sort)
    ~f:(fun (nbs, count) ->
      printf "%d N's; %d\n%!" nbs count
    );


