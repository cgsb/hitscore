
open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

module Assemble_sample_sheet:
  Hitscore_function_interfaces.ASSEMBLE_SAMPLE_SHEET
  = struct

    let illumina_barcodes = [
      1, "ATCACG"; 2, "CGATGT"; 3, "TTAGGC"; 4, "TGACCA"; 5, "ACAGTG";
      6, "GCCAAT"; 7, "CAGATC"; 8, "ACTTGA"; 9, "GATCAG"; 10, "TAGCTT";
      11, "GGCTAC"; 12, "CTTGTA"; 13, "AGTCAA"; 14, "AGTTCC"; 15, "ATGTCA";
      16, "CCGTCC"; 17, "GTAGAG"; 18, "GTCCGC"; 19, "GTGAAA"; 20, "GTGGCC";
      21, "GTTTCG"; 22, "CGTACG"; 23, "GAGTGG"; 24, "GGTAGC"; 25, "ACTGAT";
      27, "ATTCCT";
    ]
    let bioo_barcodes = [
      1,"CGATGT"; 2,"TGACCA"; 3,"ACAGTG"; 4,"GCCAAT"; 5,"CAGATC";
      6,"CTTGTA"; 7,"ATCACG"; 8,"TTAGGC"; 9,"ACTTGA"; 10,"GATCAG";
      11,"TAGCTT"; 12,"GGCTAC"; 13,"AGTCAA"; 14,"AGTTCC"; 15,"ATGTCA";
      16,"CCGTCC"; 17,"GTAGAG"; 18,"GTCCGC"; 19,"GTGAAA"; 20,"GTGGCC";
      21,"GTTTCG"; 22,"CGTACG"; 23,"GAGTGG"; 24,"GGTAGC"; 25,"ACTGAT";
      26,"ATGAGC"; 27,"ATTCCT"; 28,"CAAAAG"; 29,"CAACTA"; 30,"CACCGG";
      31,"CACGAT"; 32,"CACTCA"; 33,"CAGGCG"; 34,"CATGGC"; 35,"CATTTT";
      36,"CCAACA"; 37,"CGGAAT"; 38,"CTAGCT"; 39,"CTATAC"; 40,"CTCAGA";
      41,"GCGCTA"; 42,"TAATCG"; 43,"TACAGC"; 44,"TATAAT"; 45,"TCATTC";
      46,"TCCCGA"; 47,"TCGAAG"; 48,"TCGGCA";]

    let barcode_sequences barcodes =
      match barcodes with
      | [`illumina_i1 s] -> [s]
      | [`illumina_i1 s1; `illumina_i2 s2] -> [sprintf "%s-%s" s1 s2]
      | [`illumina_i2 s2; `illumina_i1 s1] -> [sprintf "%s-%s" s1 s2]
      | _ -> []

    let all_barcode_sequences barcode_type =
      match barcode_type with
      | Some "illumina" -> Array.of_list illumina_barcodes
      | Some "bioo" -> Array.of_list bioo_barcodes
      | _ -> [| |]

    type sample_sheet = {
      content: Buffer.t;
      kind: Layout.Enumeration_sample_sheet_kind.t;
      flowcell: Layout.Record_flowcell.pointer;
      flowcell_name: string;
    }

    let preparation
        ?(force_new=false)
        ?(kind: Layout.Enumeration_sample_sheet_kind.t=`specific_barcodes)
        ~dbh
        flowcell_name =
      let layout = Classy.make dbh in
      let flowcell_lanes =
        layout#flowcell#all
        >>| List.filter ~f:(fun f -> f#serial_name = flowcell_name)
        >>= function
        | [ one ] ->
          return (one#g_pointer,
                  Array.mapi one#lanes
                    ~f:(fun i lane_p -> i, lane_p#pointer) |! Array.to_list)
        | [] -> error (`wrong_request (`record_flowcell,
                                       `value_not_found flowcell_name))
        | l -> error (`layout_inconsistency
                         (`Record "flowcell",
                          `search_flowcell_by_name_not_unique
                            (flowcell_name, List.length l)))
      in
      let previous_assembly flowcell =
        layout#assemble_sample_sheet#all
        >>| List.filter ~f:(fun f ->
          f#flowcell#pointer = flowcell && f#g_status = `Succeeded && f#kind = kind)
        >>= (function
        | fone :: _ ->
          begin match fone#g_result with
          | None ->
            error (`layout_inconsistency (`Function "assemble_sample_sheet",
                                          `successful_status_with_no_result fone#g_id))
          | Some s ->
            return (Some (fone#g_pointer, s#pointer))
          end
        | _ -> return None)
      in
      flowcell_lanes
      >>= (fun (flowcell, lane_list) ->
        (if force_new then return None else previous_assembly flowcell)
        >>= function
        | None ->
          let out_buf = Buffer.create 42 in
          let out = Buffer.add_string out_buf in
          let print = ksprintf in
          print out "FCID,Lane,SampleID,SampleRef,Index,Description,Control\
                    ,Recipe,Operator,SampleProject\n";
          while_sequential lane_list ~f:(fun (lane_idx, l) ->
            let open Layout.Record_lane in
            Access.Lane.get ~dbh l
            >>= fun {g_value = { libraries; _ }} ->
            begin match kind with
            | `specific_barcodes ->
              while_sequential (Array.to_list libraries) ~f:(fun input ->
                let open Layout.Record_input_library in
                Access.Input_library.get ~dbh input
                >>= fun {g_value = { library; _ }} ->
                let open Layout.Record_stock_library in
                Access.Stock_library.get ~dbh library
                >>= fun {g_value = { name; barcoding ; _ }} ->
                begin match barcoding with
                | [| and_array |] ->
                  let open Layout.Record_barcode in
                  while_sequential (Array.to_list and_array) (fun one ->
                    Access.Barcode.get ~dbh one
                    >>= begin function
                    (*
                    | {g_value = { kind = `bioo; index = Some b; _}; _} ->
                      return (Some (`bioo b))
                    | {g_value = { kind = `illumina; index = Some b; _}; _} ->
                      return (Some (`illumina b))
                    *)
                    | {g_value = { read = Some "I1";
                                   position = Some 1; sequence = Some s; _};
                       _} ->
                      return (Some (`illumina_i1 s))
                    | {g_value = { read = Some "I2";
                                   position = Some 1; sequence = Some s; _};
                       _} ->
                      return (Some (`illumina_i2 s))
                    | _ -> return None
                    end)
                  >>| List.filter_opt
                | _ -> return ([])
                end
                >>= fun illuminaish_barcoding ->
                let barcodes =
                  (* if only one lib, then no barcoding *)
                  if Array.length libraries <= 1 then []
                  else illuminaish_barcoding in
                return (name, (barcode_sequences barcodes)))
              >>= fun name_bars ->
              begin match name_bars with
              | [] ->
                let name =
                  match name_bars with
                  | [ (one_name, _) ] -> one_name
                  | _ -> sprintf "PoolLane%d" (lane_idx + 1)
                in
                print out "%s,%d,%s,,,,N,,,Lane%d\n"
                  flowcell_name (lane_idx + 1) name (lane_idx + 1);
                return ()
              | [(name, _)] ->
                (* A library alone in its lane does not get demultiplexed,
                   but we use its name for the “undetermined” *)
                print out "%s,%d,%s,,Undetermined,,N,,,Lane%d\n"
                  flowcell_name (lane_idx + 1) name (lane_idx + 1);
                return ()
              | l ->
                print out "%s,%d,UndeterminedLane%d,,Undetermined,,N,,,Lane%d\n"
                  flowcell_name (lane_idx + 1) (lane_idx + 1) (lane_idx + 1);
                let potential_dup =
                  List.find_a_dup ~compare
                    (List.map l (fun s -> snd s) |! List.concat) in
                begin match potential_dup with
                | Some b -> error (`duplicated_barcode b)
                | None -> return ()
                end
                >>= fun () ->
                while_sequential l ~f:(fun (name, bars) ->
                  List.iter bars ~f:(fun b ->
                    print out "%s,%d,%s,,%s,,N,,,Lane%d\n"
                      flowcell_name (lane_idx + 1) name b (lane_idx + 1));
                  return ())
                >>= fun _ -> return ()
              end

            | `all_barcodes ->
              begin match libraries with
              | [| |] ->
                print out "%s,%d,SingleSample%d,,,,N,,,Lane%d\n"
                  flowcell_name (lane_idx + 1) (lane_idx + 1) (lane_idx + 1);
                return ()
              | some ->
                while_sequential (Array.to_list some) begin fun il ->
                  let open Layout.Record_input_library in
                  Access.Input_library.get ~dbh il
                  >>= fun {g_value = { library; _ }} ->
                  let open Layout.Record_stock_library in
                  Access.Stock_library.get ~dbh library
                  >>= fun {g_value = { barcoding; _ }} ->
                  while_sequential
                    (List.concat (Array.(map ~f:to_list barcoding |! to_list)))
                    begin fun b ->
                      let open Layout.Record_barcode in
                      Access.Barcode.get ~dbh b
                      >>= fun {g_value = {provider ; _}} ->
                      return provider
                    end
                end
                >>| List.concat
                >>| List.filter ~f:(fun k -> k = Some "bioo" || k = Some "illumina")
                >>| List.dedup
                >>= fun barcode_types ->
                begin match barcode_types with
                | [Some one_barcode_type] -> return (Some one_barcode_type)
                | _ -> return (None)
                end
                >>= fun elected_barcode_type ->
                begin match (all_barcode_sequences elected_barcode_type) with
                | [| |] ->
                  print out "%s,%d,SingleSample%d,,,,N,,,Lane%d\n"
                    flowcell_name (lane_idx + 1) (lane_idx + 1) (lane_idx + 1);
                  return ()
                | some ->
                  Array.iter some  ~f:(fun (i, b) ->
                    print out "%s,%d,Lane%d_%s_%02d_%s,,%s,,N,,,Lane%d\n"
                      flowcell_name (lane_idx + 1) (lane_idx + 1)
                      (Option.value ~default:"NONE" elected_barcode_type)
                      i b b (lane_idx + 1));
                  return ()
                end
              end
            | `no_demultiplexing ->
              print out "%s,%d,UndeterminedLane%d,,Undetermined,,N,,,Lane%d\n"
                flowcell_name (lane_idx + 1) (lane_idx + 1) (lane_idx + 1);
              return ()
            end)
          >>= fun (_: unit list) ->
          return (`new_one { content = out_buf; kind; flowcell; flowcell_name })
        | Some (assembly, sample_sheet) ->
          return (`old_one (assembly, sample_sheet))
      )

    let get_target_file ~dbh sample_sheet =
      let open Layout.File_system in
      let files = Tree.([file "SampleSheet.csv"]) in
      Access.Volume.add_tree_volume ~dbh
        ~hr_tag:(sprintf "%s_%s" sample_sheet.flowcell_name
                   (Layout.Enumeration_sample_sheet_kind.to_string
                      sample_sheet.kind))
        ~kind:`sample_sheet_csv ~files
      >>= fun file ->
      Access.Volume.get ~dbh file
      >>= function
      | { g_id = id ; g_kind = kind;
          g_content = Tree (hr_tag, trees) } ->
        begin match Common.trees_to_unix_relative_paths files with
        | [ one_path ] ->
          return (file, Common.volume_unix_directory ~id ~kind ?hr_tag, one_path)
        | _ ->
          error (`fatal_error `trees_to_unix_paths_should_return_one)
        end
      | _ ->
        error (`fatal_error (`add_volume_did_not_create_a_tree_volume file))


    let register_with_success ~dbh ?note ~file sample_sheet =
      Access.Assemble_sample_sheet.add_evaluation
        ~kind:sample_sheet.kind ~dbh ~flowcell:sample_sheet.flowcell
      >>= fun assembly ->
      Access.Sample_sheet.add_value ~file ?note ~dbh
      >>= fun result ->
      Access.Assemble_sample_sheet.set_succeeded ~dbh
        ~result assembly
      >>= fun () ->
      return assembly

    let register_with_failure ~dbh ?note sample_sheet =
      Access.Assemble_sample_sheet.add_evaluation
        ~kind:sample_sheet.kind ~dbh ~flowcell:sample_sheet.flowcell
      >>= fun assembly ->
      Access.Assemble_sample_sheet.set_failed ~dbh assembly
      >>= fun () ->
      return assembly

    let run ~dbh ~kind ~configuration ?force_new ?note flowcell =
      preparation ~kind ~dbh ?force_new flowcell
      >>= function
        | `new_one sample_sheet ->
          get_target_file ~dbh sample_sheet
          >>= fun (the_volume, path_vol, path_file) ->
          let commands_m =
            match Configuration.path_of_volume configuration path_vol with
            | Some vol_dir ->
              ksprintf system_command "mkdir -p %s/" vol_dir >>= fun () ->
              write_file (sprintf "%s/%s" vol_dir path_file)
                ~content:(Buffer.contents sample_sheet.content)
              >>= fun () ->
              Access_rights.set_posix_acls ~dbh (`dir vol_dir) ~configuration
            | None -> error `root_directory_not_configured
          in
          double_bind commands_m
            ~ok:(fun () ->
              register_with_success ~dbh ?note ~file:the_volume sample_sheet
              >>= fun succeeded ->
              Access.Log.add_value ~dbh
                ~log:(sprintf "(assemble_sample_sheet_success %d %s %s)"
                        succeeded.Layout.Function_assemble_sample_sheet.id
                        flowcell
                        (Layout.Enumeration_sample_sheet_kind.to_string kind))
              >>= fun _ ->
              return (`new_success succeeded))
            ~error:(fun e ->
              register_with_failure ~dbh ?note sample_sheet
              >>= fun failed ->
              Access.Log.add_value ~dbh
                ~log:(sprintf "(assemble_sample_sheet_failure %d %s %s)"
                        failed.Layout.Function_assemble_sample_sheet.id
                        flowcell
                        (Layout.Enumeration_sample_sheet_kind.to_string kind))
              >>= fun _ ->
              Access.Volume.(
                get ~dbh the_volume
                >>= fun vol ->
                delete_volume_unsafe ~dbh vol
                >>= fun () ->
                return vol.Layout.File_system.g_id)
              >>= fun id ->
              Access.Log.add_value ~dbh
                ~log:(sprintf "(delete_orphan_volume (id %d))" id)
              >>= fun _ ->
              return (`new_failure (failed, e)))

        | `old_one (assembly, sample_sheet) ->
          Access.Log.add_value ~dbh
            ~log:(sprintf "(assemble_sample_sheet_reuse %d %s %s)"
                    assembly.Layout.Function_assemble_sample_sheet.id
                    flowcell
                    (Layout.Enumeration_sample_sheet_kind.to_string kind))
          >>= fun _ ->
          return (`previous_success (assembly, sample_sheet))



end
