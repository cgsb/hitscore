open Hitscore_std


exception Local_wrong_field of string

let run_parameters (xml : Hitscore_interfaces.XML.tree) =
  let read1 = ref None in
  let read2 = ref None in
  let idx_read = ref None in
  let intensities_kept = ref None in
  let flowcell = ref None in
  let start_date = ref None in
  let ios i = Option.try_with (fun () -> Int.of_string i) in
  let bos b = Option.try_with (fun () -> Bool.of_string b) in

  let rec go_through = function
    | `E (((_,"Read1"), _), [ `D i ]) -> read1 := ios i 
    | `E (((_,"Read2"), _), [ `D i ]) -> read2 := ios i
    | `E (((_,"KeepIntensityFiles"), _), [ `D b ]) -> intensities_kept := bos b
    | `E (((_,"IndexRead"), _), [ `D i ]) -> idx_read := ios i
    | `E (((_,"Barcode"), _), [ `D s ]) -> flowcell := Some s
    | `E (((_,"RunStartDate"), _), [ `D s ]) ->
      let scanned =
        Scanf.sscanf s "%2d%2d%2d"
          (sprintf "20%d-%02d-%02d 09:00:00.000000-05:00") in
      start_date := Some (Time.of_string scanned)
    | `E (t, tl) -> List.iter tl go_through
    | `D s -> ()
  in
  let gv m = function None -> raise (Local_wrong_field m) | Some s -> s in
  begin try
    go_through  xml;
    Ok (gv "flowcell" !flowcell,
        gv "read1" !read1,
        !idx_read,
        !read2,
        gv "intensities_kept" !intensities_kept,
        gv "start_date" !start_date)
    with
    | Local_wrong_field m ->
      Error (`parse_run_parameters (`wrong_field m))
    | Scanf.Scan_failure s ->
      Error (`parse_run_parameters (`wrong_date s))
  end


type clusters_summary = (float * float * float * float * float * float) list

let clusters_summary xml =

  let result = ref [] in
  let current_key = ref 0 in
  
  let rec go_through = function
    | `E (((_,"Summary"), attrs), more) -> 
      List.iter ~f:go_through more
    | `E (((_,"Lane"), attrs), more) ->
      let c (x, y) = printf "%s: %s\n" x y in
      let fos s = try Some (Float.of_string s) with e -> None in
      let clusters_raw       = ref None in
      let clusters_raw_sd    = ref None in
      let clusters_pf        = ref None in
      let clusters_pf_sd     = ref None in
      let prc_pf_clusters    = ref None in
      let prc_pf_clusters_sd = ref None in
      List.iter attrs (fun ((_, k), v) ->
        match k, v with
        | "key", nb -> 
          current_key := Int.of_string nb
        | "ClustersRaw", nb     -> clusters_raw       := fos nb 
        | "ClustersRawSD", nb   -> clusters_raw_sd    := fos nb 
        | "ClustersPF", nb      -> clusters_pf        := fos nb 
        | "ClustersPFSD", nb    -> clusters_pf_sd     := fos nb 
        | "PrcPFClusters", nb   -> prc_pf_clusters    := fos nb 
        | "PrcPFClustersSD", nb -> prc_pf_clusters_sd := fos nb 
        | "TileCount", nb                     as x -> c x
        | "Phasing", nb                       as x -> c x
        | "Prephasing", nb                    as x -> c x
        | "CalledCyclesMin", nb               as x -> c x
        | "CalledCyclesMax", nb               as x -> c x
        | "PrcAlign", nb                      as x -> c x
        | "PrcAlignSD", nb                    as x -> c x
        | "ErrRatePhiX", nb                   as x -> c x
        | "ErrRatePhiXSD", nb                 as x -> c x
        | "ErrRate35", nb                     as x -> c x
        | "ErrRate35SD", nb                   as x -> c x
        | "ErrRate75", nb                     as x -> c x
        | "ErrRate75SD", nb                   as x -> c x
        | "ErrRate100", nb                    as x -> c x
        | "ErrRate100SD", nb                  as x -> c x
        | "FirstCycleIntPF", nb               as x -> c x
        | "FirstCycleIntPFSD", nb             as x -> c x
        | "PrcIntensityAfter20CyclesPF", nb   as x -> c x
        | "PrcIntensityAfter20CyclesPFSD", nb as x -> c x
        | _ -> ());
      let v = Option.value_exn_message in
      result := (
        v "clusters_info: Missing clusters_raw      " !clusters_raw       ,
        v "clusters_info: Missing clusters_raw_sd   " !clusters_raw_sd    ,
        v "clusters_info: Missing clusters_pf       " !clusters_pf        ,
        v "clusters_info: Missing clusters_pf_sd    " !clusters_pf_sd     ,
        v "clusters_info: Missing prc_pf_clusters   " !prc_pf_clusters    ,
        v "clusters_info: Missing prc_pf_clusters_sd" !prc_pf_clusters_sd )
      :: !result;
      List.iter ~f:go_through more
    | `E (_, more) ->
      List.iter ~f:go_through more
    | `D s -> ()
  in
  try
    go_through xml;
    Ok (List.rev !result)
  with
  | Failure m -> Error (`parse_clusters_summary m)
