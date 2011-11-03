#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "batteries"; "biocaml"];;
--

open Biocaml_std
module Fastq = Biocaml_fastq
module HT = struct
  include BatHashtbl
  include BatHashtbl.Labels
  include BatHashtbl.Exceptionless
end

let () =
  let dir = if Array.length Sys.argv > 1 then Sys.argv.(1) else "." in
  let find_stream = 
    BatUnix.open_process_in (sprintf "find %s -name \"*.fastq.gz\"" dir) in
  let find_enum = IO.lines_of find_stream in


  let results = HT.create 42 in

  Enum.iter find_enum ~f:(fun line ->
    eprintf "Play with %s\n%!" line;
    let i = BatUnix.open_process_in (sprintf "gunzip -c %s" line) in
    let fastq = Fastq.enum_input i in
    let nb = Enum.hard_count fastq in
    IO.close_in i;
    match String.nsplit (Filename.basename line) "_" with
    | [ sample_name; barcode; lane; side; nb_extension ] ->
      let key = (sample_name, lane, side) in
      begin match HT.find results key with
      | Some count ->
        HT.replace results key (count + nb)
      | None -> 
        HT.add results key nb
      end
    | _ -> eprintf "Did not manage to understand file %s" line
  );
  
  List.iter (results |> HT.enum |> List.of_enum |> List.fast_sort)
    ~f:(fun ((name, lane, side), count) ->
      printf "Sample %-20s on lane %s (%s): %d\n%!" name lane side count;
    )

