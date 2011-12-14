
module Make
  (Result_IO : Hitscore_result_IO.RESULT_IO) 
  (Layout: module type of Hitscore_db_access.Make(Result_IO)) = struct

    open Hitscore_std
    open Result_IO

    let illumina_barcodes = [
      1l, "ATCACG"; 2l, "CGATGT"; 3l, "TTAGGC"; 4l, "TGACCA"; 5l, "ACAGTG";
      6l, "GCCAAT"; 7l, "CAGATC"; 8l, "ACTTGA"; 9l, "GATCAG"; 10l, "TAGCTT";
      11l, "GGCTAC"; 12l, "CTTGTA"
    ]
    let bioo_barcodes = [
      1l,"CGATGT"; 2l,"TGACCA"; 3l,"ACAGTG"; 4l,"GCCAAT"; 5l,"CAGATC";
      6l,"CTTGTA"; 7l,"ATCACG"; 8l,"TTAGGC"; 9l,"ACTTGA"; 10l,"GATCAG";
      11l,"TAGCTT"; 12l,"GGCTAC"; 13l,"AGTCAA"; 14l,"AGTTCC"; 15l,"ATGTCA";
      16l,"CCGTCC"; 17l,"GTAGAG"; 18l,"GTCCGC"; 19l,"GTGAAA"; 20l,"GTGGCC";
      21l,"GTTTCG"; 22l,"CGTACG"; 23l,"GAGTGG"; 24l,"GGTAGC"; 25l,"ACTGAT";
      26l,"ATGAGC"; 27l,"ATTCCT"; 28l,"CAAAAG"; 29l,"CAACTA"; 30l,"CACCGG";
      31l,"CACGAT"; 32l,"CACTCA"; 33l,"CAGGCG"; 34l,"CATGGC"; 35l,"CATTTT";
      36l,"CCAACA"; 37l,"CGGAAT"; 38l,"CTAGCT"; 39l,"CTATAC"; 40l,"CTCAGA";
      41l,"GCGCTA"; 42l,"TAATCG"; 43l,"TACAGC"; 44l,"TATAAT"; 45l,"TCATTC";
      46l,"TCCCGA"; 47l,"TCGAAG"; 48l,"TCGGCA";] 

    let barcode_sequences barcode_type barcodes =
      let barcode_assoc = 
        match barcode_type with
        | `none | `bioo_8_beginning | `nugen -> []
        | `illumina -> illumina_barcodes
        | `bioo -> bioo_barcodes in          
      let module M = struct
        exception Local_barcode_not_found of int32
        let f () =
          try Ok (
            if barcode_assoc <> [] then
              Array.map barcodes ~f:(fun idx ->
                match List.Assoc.find barcode_assoc idx with
                | Some b -> b
                | None -> raise (Local_barcode_not_found idx))
            else 
              [| "" |])
          with
          | Local_barcode_not_found i ->
            Error (`barcode_not_found (i, barcode_type))
      end in
      M.f ()

    let all_barcode_sequences barcode_type =
      match barcode_type with
      | `none | `bioo_8_beginning | `nugen -> [| |]
      | `illumina -> Array.of_list illumina_barcodes
      | `bioo -> Array.of_list bioo_barcodes 

    type sample_sheet = {
      content: Buffer.t;
      kind: Layout.Enumeration_sample_sheet_kind.t;
      flowcell: Layout.Record_flowcell.t;
      flowcell_name: string;
    }

    let preparation
        ?(kind: Layout.Enumeration_sample_sheet_kind.t=`specific_barcodes)
        ~dbh
        flowcell_name =
      let flowcell_lanes =
        wrap_pgocaml
          ~query:(fun () ->
            let open Layout in
            let open Batteries in
            PGSQL (dbh) 
              "select g_id, lanes from flowcell where serial_name = $flowcell_name")
          ~on_result:(function
            | [ id, array_of_ids ] ->
              return ({ Layout.Record_flowcell.id },
                      Array.mapi array_of_ids 
                        ~f:(fun i id -> i, { Layout.Record_lane.id })
                         |> Array.to_list)
            | [] -> error (`wrong_request (`record_flowcell, 
                                           `value_not_found flowcell_name))
            | l -> error (`layout_inconsistency (`record_flowcell,
                                                 `search_by_name_not_unique l)))
      in
      let previous_assembly flowcell =
        wrap_pgocaml
          ~query:(fun () ->
            let open Layout in
            let open Batteries in
            let flowcell_id = flowcell.Layout.Record_flowcell.id in
            let kind_str =
              Layout.Enumeration_sample_sheet_kind.to_string kind in
            PGSQL (dbh) 
              "select g_id, g_result from assemble_sample_sheet \
               where flowcell = $flowcell_id and g_status = 'Succeeded' \
                and kind = $kind_str")
          ~on_result:(function
            | (idf, Some idr) :: _ ->
              return (Some (
                Layout.Function_assemble_sample_sheet.(
                  ({ id = idf } : [ `can_get_result ] t)),
                Layout.Record_sample_sheet.({ id = idr })))
            | (idf, None) :: _ ->
              error (`layout_inconsistency (`function_assemble_sample_sheet,
                                            `successful_status_with_no_result idf))
            | _ -> return None)
      in
      flowcell_lanes
      >>= (fun (flowcell, lane_list) ->
        previous_assembly flowcell 
        >>= function
        | None ->
          let out_buf = Buffer.create 42 in
          let out = Buffer.add_string out_buf in
          let print = ksprintf in
          print out "FCID,Lane,SampleID,SampleRef,Index,Description,Control\
                    ,Recipe,Operator,SampleProject\n";
          of_list_sequential lane_list ~f:(fun (lane_idx, l) ->
            let open Layout.Record_lane in
            cache_value ~dbh l
            >>| get_fields
            >>= fun { libraries; _ } -> 
            begin match kind with
            | `specific_barcodes ->
              of_list_sequential (Array.to_list libraries) ~f:(fun input ->
                let open Layout.Record_input_library in
                cache_value ~dbh input >>| get_fields 
                >>= fun { library; _ } ->
                let open Layout.Record_stock_library in
                cache_value ~dbh library >>| get_fields
                >>= fun { name; barcode_type; barcodes; _ } ->
                let barcode_type =
                (* if only one lib, then no barcoding *)
                  if Array.length libraries <= 1 then `none else barcode_type in
                begin match (barcode_sequences barcode_type barcodes) with
                | Ok bars ->
                  Array.iter bars ~f:(fun b ->
                    print out "%s,%d,%s,,%s,,N,,,Lane%d\n"
                      flowcell_name (lane_idx + 1) name b (lane_idx + 1));
                  return ()
                | Error e -> error e
                end)
            | `all_barcodes ->
              begin match libraries with
              | [| |] -> 
                print out "%s,%d,SingleSample%d,,,,N,,,Lane%d\n"
                  flowcell_name (lane_idx + 1) (lane_idx + 1) (lane_idx + 1);
                return []
              | some ->
                let open Layout.Record_input_library in
                cache_value ~dbh some.(0) >>| get_fields 
                >>= fun { library; _ } ->
                let open Layout.Record_stock_library in
                cache_value ~dbh library >>| get_fields
                >>= fun { barcode_type; _ } ->
                Array.iter (all_barcode_sequences barcode_type) ~f:(fun (i, b) ->
                  print out "%s,%d,Lane%d_%s_%02ld_%s,,%s,,N,,,Lane%d\n"
                    flowcell_name (lane_idx + 1) (lane_idx + 1) 
                    (Layout.Enumeration_barcode_provider.to_string barcode_type)
                    i b b (lane_idx + 1));
                return []
              end
            end) 
          >>= fun _ -> return (`new_one 
                                  { content = out_buf; kind; flowcell; 
                                    flowcell_name })
        | Some (assembly, sample_sheet) -> return (`old_one (assembly, sample_sheet))
      )

    let output samplesheet output_string =
      output_string (Buffer.contents samplesheet.content)

    let get_target_file ~dbh sample_sheet =
      let files = Layout.File_system.Tree.([file "SampleSheet.csv"]) in
      Layout.File_system.add_volume ~dbh
        ~hr_tag:(sprintf "%s_%s" sample_sheet.flowcell_name 
                   (Layout.Enumeration_sample_sheet_kind.to_string
                      sample_sheet.kind))
        ~kind:`sample_sheet_csv ~files
      >>= fun file ->
      Layout.File_system.cache_volume_entry ~dbh file 
      >>= fun vol_entry_cache ->
      Layout.File_system.(
        match trees_to_unix_paths files with
        | [ one_path ] ->
          return (file, entry_unix_path (volume_entry vol_entry_cache), one_path)
        | _ ->
          error (`fatal_error `trees_to_unix_paths_should_return_one))


    let register_with_success ~dbh ?note ~file sample_sheet =
      Layout.Function_assemble_sample_sheet.add_evaluation
        ~recomputable:true ~recompute_penalty:1.
        ~kind:sample_sheet.kind ~dbh ~flowcell:sample_sheet.flowcell
      >>= fun assembly ->
      Layout.Record_sample_sheet.add_value ~file ?note ~dbh
      >>= fun result ->
      Layout.Function_assemble_sample_sheet.set_succeeded ~dbh
        ~result assembly

    let register_with_failure ~dbh ?note sample_sheet =
      Layout.Function_assemble_sample_sheet.add_evaluation
        ~recomputable:true ~recompute_penalty:1.
        ~kind:sample_sheet.kind ~dbh ~flowcell:sample_sheet.flowcell
      >>= fun assembly ->
      Layout.Function_assemble_sample_sheet.set_failed ~dbh assembly

    let run ~dbh ~kind ?note ~write_to_tmp ~mv_from_tmp flowcell =
      preparation ~kind ~dbh flowcell
      >>= function
        | `new_one sample_sheet ->
          output sample_sheet write_to_tmp
          >>= fun () ->
          get_target_file ~dbh sample_sheet
          >>= fun (the_volume, pathd, pathf) ->
          let movem = mv_from_tmp pathd pathf in
          double_bind movem
            ~ok:(fun () ->
              register_with_success ~dbh ?note ~file:the_volume sample_sheet
              >>= fun succeeded ->
              Layout.Record_log.add_value ~dbh
                ~log:(sprintf "(assemble_sample_sheet_success %ld %s %s)"
                        succeeded.Layout.Function_assemble_sample_sheet.id
                        flowcell
                        (Layout.Enumeration_sample_sheet_kind.to_string kind))
              >>= fun _ ->
              return (`new_success succeeded))
            ~error:(fun e ->
              register_with_failure ~dbh ?note sample_sheet 
              >>= fun failed ->
              Layout.Record_log.add_value ~dbh
                ~log:(sprintf "(assemble_sample_sheet_failure %ld %s %s)"
                        failed.Layout.Function_assemble_sample_sheet.id
                        flowcell
                        (Layout.Enumeration_sample_sheet_kind.to_string kind))
              >>= fun _ ->
              Layout.Record_log.add_value ~dbh
                ~log:(sprintf "(orphan_volume %ld %s (%s))"
                        the_volume.Layout.File_system.id
                        pathd pathf)
              >>= fun _ ->
              return (`new_failure (failed, e)))

        | `old_one (assembly, sample_sheet) -> 
          Layout.Record_log.add_value ~dbh
            ~log:(sprintf "(assemble_sample_sheet_reuse %ld %s %s)"
                    assembly.Layout.Function_assemble_sample_sheet.id
                    flowcell
                    (Layout.Enumeration_sample_sheet_kind.to_string kind))
          >>= fun _ ->
          return (`previous_success (assembly, sample_sheet))



end
