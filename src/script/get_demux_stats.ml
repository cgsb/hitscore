#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "batteries"; "hitscore"];;
--

open Hitscore_std
module HT =  BatHashtbl


let () =
  let samples = HT.create 42 in
  let file = Sys.argv.(1) in
  let i = Xmlm.make_input (`Channel (Pervasives.open_in file)) in
  let current_sample = ref ("", "") in
  let current_lane = ref "" in
  let mm0 = ref false in
  let mm1 = ref false in
  let in_raw = ref false in
  let in_pf = ref false in
  let pull i =
    match Xmlm.input i with 
    | `El_start ((_, "Lane"), [ (_, "index"), name ]) ->
      current_lane := name
    | `El_start ((_, "Sample"), [ (_, "index"), name ]) ->
      current_sample := (!current_lane, name);
      HT.add samples !current_sample (0, 0, 0, 0)
    | `El_start ((_, "ClusterCount0MismatchBarcode"), _) ->
      mm0 := true
    | `El_start ((_, "ClusterCount1MismatchBarcode"), _) ->
      mm1 := true
    | `El_start ((_, "Pf"), _) ->
      in_raw := false; in_pf := true
    | `El_start ((_, "Raw"), _) ->
      in_raw := true; in_pf := false
    | `El_start ((_, s), _) -> ()
    | `El_end -> mm0 := false; mm1 := false
    | `Data s -> 
      if !in_raw && !mm0 then (
        let rm0, rm1, pf0, pf1 = HT.find samples !current_sample in
        HT.replace samples !current_sample (rm0 + (int_of_string s), rm1, pf0, pf1);
      );
      if !in_raw && !mm1 then (
        let rm0, rm1, pf0, pf1 = HT.find samples !current_sample in
        HT.replace samples !current_sample (rm0, rm1 + (int_of_string s), pf0, pf1);
      );
      if !in_pf && !mm0 then (
        let rm0, rm1, pf0, pf1 = HT.find samples !current_sample in
        HT.replace samples !current_sample (rm0, rm1, pf0 + (int_of_string s), pf1);
      );
      if !in_pf && !mm1 then (
        let rm0, rm1, pf0, pf1 = HT.find samples !current_sample in
        HT.replace samples !current_sample (rm0, rm1, pf0, pf1 + (int_of_string s));
      );
    | `Dtd _ -> ()
  in
  while not (Xmlm.eoi i) do pull i done;
  let sorted =
    HT.enum samples |> List.of_enum |> List.sort in
  printf "{begin table 6 readtable r}\n";
  printf "{c h|Lane} {c h|Sample} {c h|Raw Mismatch 0} {c h|Raw Mismatch 1}\
          {c h|PF Mismatch 0} {c h|PF Mismatch 1}";
  List.iter (fun ((l, s), (rm0, rm1, pf0, pf1)) ->
    printf "{c|%s}{c|%s}{c|{t|%d}}{c|{t|%d}}{c|{t|%d}}{c|{t|%#d}}\n"
      l s rm0 (rm0 + rm1) pf0 (pf0 + pf1)) sorted;
  printf "{end}\n"


