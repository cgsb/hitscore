open Hitscore_std
(*
type library_stats = {
  name: string;
  mutable yield: float;
  mutable yield_q30: float;
  mutable cluster_count: float;
  mutable cluster_count_m0: float;
  mutable cluster_count_m1: float;
  mutable quality_score_sum: float;
}*)
let new_lib_read name =
  {
    Hitscore_interfaces.B2F_unaligned_information.
    name;
    yield = 0.;
    yield_q30 = 0.;
    cluster_count = 0.;
    cluster_count_m0 = 0.;
    cluster_count_m1 = 0.;
    quality_score_sum = 0.;
  }
(*
type demux_summary = library_stats list array
*)
let flowcell_demux_summary (xml : Hitscore_interfaces.XML.tree) =
  let current_lane = ref 0 in
  (* let current_sample = ref "" in *)
  (* let current_read = ref 0 in *)

  let in_pf = ref false in

  let lanes = Array.create 8 [] in

  let add_here f =
    if !in_pf then f (List.hd_exn lanes.(!current_lane - 1)) 
  in
  let open Hitscore_interfaces.B2F_unaligned_information in

  let rec go_through = function
    | `E (((_, "Lane"), [ (_, "index"), index]), more) ->
      current_lane := Int.of_string index;
      List.iter more go_through
    | `E (((_, "Sample"), [ (_, "index"), index]), more) ->
      lanes.(!current_lane - 1) <- new_lib_read index :: lanes.(!current_lane - 1);
      List.iter more go_through
    | `E (((_, "Pf"), _), more) ->
      in_pf := true;
      List.iter more go_through;
      in_pf := false;
    | `E (((_, "Yield"), _), [`D nb]) ->
      add_here (fun r -> r.yield <- r.yield +. Float.of_string nb);
    | `E (((_, "YieldQ30"), _), [`D nb]) ->
      add_here (fun r -> r.yield_q30 <- r.yield_q30 +. Float.of_string nb);
    | `E (((_, "ClusterCount"), _), [`D nb]) ->
      add_here (fun r -> r.cluster_count <- r.cluster_count +. Float.of_string nb);
    | `E (((_, "ClusterCount0MismatchBarcode"), _), [`D nb]) ->
      add_here (fun r -> 
        r.cluster_count_m0 <- r.cluster_count_m0 +. Float.of_string nb);
    | `E (((_, "ClusterCount1MismatchBarcode"), _), [`D nb]) ->
      add_here (fun r -> 
        r.cluster_count_m1 <- r.cluster_count_m1 +. Float.of_string nb);
    | `E (((_, "QualityScoreSum"), _), [`D nb]) ->
      add_here (fun r -> 
        r.quality_score_sum <- r.quality_score_sum +. Float.of_string nb);
    | `E (t, tl) -> List.iter tl go_through
    | `D s -> ()
  in
  begin 
    try
      go_through  xml;
      Ok (Array.map lanes ~f:(List.sort ~cmp:Pervasives.compare))
    with
    | e -> Error (`parse_flowcell_demux_summary_error e)
  end
