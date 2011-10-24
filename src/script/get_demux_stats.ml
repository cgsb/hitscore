#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "batteries"; "hitscore"];;
--

open Hitscore_std
module HT =  BatHashtbl

type sample_stats = {
  mutable raw_yield               : float;
  mutable raw_yield_q30           : float;
  mutable raw_cluster_count       : float;
  mutable raw_cluster_count_m0    : float;
  mutable raw_cluster_count_m1    : float;
  mutable raw_quality_score_sum   : float;
  mutable pf_yield                : float;
  mutable pf_yield_q30            : float;
  mutable pf_cluster_count        : float;
  mutable pf_cluster_count_m0     : float;
  mutable pf_cluster_count_m1     : float;
  mutable pf_quality_score_sum    : float;
}
let sample_stats () = {
  raw_yield             = 0.;
  raw_yield_q30         = 0.;
  raw_cluster_count     = 0.;
  raw_cluster_count_m0  = 0.;
  raw_cluster_count_m1  = 0.;
  raw_quality_score_sum = 0.;
  pf_yield              = 0.;
  pf_yield_q30          = 0.;
  pf_cluster_count      = 0.;
  pf_cluster_count_m0   = 0.;
  pf_cluster_count_m1   = 0.;
  pf_quality_score_sum  = 0.;
}


let () =
  let samples = HT.create 42 in
  let file = Sys.argv.(1) in
  let i = Xmlm.make_input (`Channel (Pervasives.open_in file)) in
  let current_sample = ref ("", "") in
  let current_lane = ref "" in
  let mm0 = ref false in
  let mm1 = ref false in
  let qss = ref false in
  let in_raw = ref false in
  let in_pf = ref false in
  let pull i =
    match Xmlm.input i with 
    | `El_start ((_, "Lane"), [ (_, "index"), name ]) ->
      current_lane := name
    | `El_start ((_, "Sample"), [ (_, "index"), name ]) ->
      current_sample := (!current_lane, name);
      HT.add samples !current_sample (sample_stats ())
    | `El_start ((_, "ClusterCount0MismatchBarcode"), _) ->
      mm0 := true
    | `El_start ((_, "ClusterCount1MismatchBarcode"), _) ->
      mm1 := true
    | `El_start ((_, "QualityScoreSum"), _) ->
      qss := true
    | `El_start ((_, "Pf"), _) ->
      in_raw := false; in_pf := true
    | `El_start ((_, "Raw"), _) ->
      in_raw := true; in_pf := false
    | `El_start ((_, s), _) -> ()
    | `El_end -> mm0 := false; mm1 := false; qss := false
    | `Data s -> 
      begin try
        let stats = HT.find samples !current_sample in
        if !in_raw && !mm0 then 
          stats.raw_cluster_count_m0 <- stats.raw_cluster_count_m0 +. (float_of_string s);
        if !in_raw && !mm1 then
          stats.raw_cluster_count_m1 <- stats.raw_cluster_count_m1 +. (float_of_string s);
        if !in_pf && !mm0 then
          stats.pf_cluster_count_m0 <- stats.pf_cluster_count_m0 +. (float_of_string s);
        if !in_pf && !mm1 then
          stats.pf_cluster_count_m1 <- stats.pf_cluster_count_m1 +. (float_of_string s);

        if !in_raw && !qss then
          stats.raw_quality_score_sum <- stats.raw_quality_score_sum +. (float_of_string s);
        if !in_pf && !qss then
          stats.pf_quality_score_sum <- stats.pf_quality_score_sum +. (float_of_string s);
        with 
        | Not_found -> ()
        | Failure s ->
          let lane, name = !current_sample in
          eprintf "Failure in Sample (%s, %s): %s" lane name s
      end

    | `Dtd _ -> ()
  in
  while not (Xmlm.eoi i) do pull i done;
  let sorted =
    HT.enum samples |> List.of_enum |> List.sort in
  printf "{begin table 8 readtable r}\n";
  printf "{c h|Lane} {c h|Sample}\
          {c h|Raw Mismatch 0} {c h|Raw Mismatch 1} {c h|Raw Quality Score Sum}\
          {c h|PF Mismatch 0} {c h|PF Mismatch 1} {c h|PF Quality Score Sum}";
  List.iter (fun ((l, s), stats) ->
    printf "{c|%s}{c|%s}{c|{t|%F}}{c|{t|%F}}{c|{t|%F}}{c|{t|%F}}{c|{t|%F}}{c|{t|%F}}\n"
      l s 
      stats.raw_cluster_count_m0
      (stats.raw_cluster_count_m0 +. stats.raw_cluster_count_m1)
      (* (0.01 *. stats.raw_quality_score_sum /. (stats.raw_cluster_count_m0 +. stats.raw_cluster_count_m1)) *)
      stats.raw_quality_score_sum
      stats.pf_cluster_count_m0
      (stats.pf_cluster_count_m0 +. stats.pf_cluster_count_m1)
      stats.pf_quality_score_sum
  (* (0.01 *. stats.pf_quality_score_sum /. (stats.pf_cluster_count_m0 +. stats.pf_cluster_count_m1)) *)
  ) sorted;
  printf "{end}\n"


