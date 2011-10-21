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
  let current_name = ref "" in
  let mm0 = ref false in
  let mm1 = ref false in
  let in_raw = ref false in
  let pull i =
    match Xmlm.input i with 
    | `El_start ((_, "Sample"), [ (_, "index"), name ]) ->
      current_name := name;
      HT.add samples name (0, 0);
      printf "\nSample: %s " name
    | `El_start ((_, "ClusterCount0MismatchBarcode"), _) ->
      mm0 := true
    | `El_start ((_, "ClusterCount1MismatchBarcode"), _) ->
      mm1 := true
    | `El_start ((_, "Pf"), _) ->
      in_raw := false
    | `El_start ((_, "Raw"), _) ->
      in_raw := true
    | `El_start ((_, s), _) -> ()
    | `El_end -> mm0 := false; mm1 := false
    | `Data s -> 
      if !in_raw && !mm0 then (
        let m0, m1 = HT.find samples !current_name in
        HT.replace samples !current_name (m0 + (int_of_string s), m1);
      );
      if !in_raw && !mm1 then (
        let m0, m1 = HT.find samples !current_name in
        HT.replace samples !current_name (m0, m1 + (int_of_string s));
      );
    | `Dtd _ -> ()
  in
  while not (Xmlm.eoi i) do pull i done;
  HT.iter (fun k (m1, m0) ->
    printf "%s %d %d\n" k m1 m0) samples


