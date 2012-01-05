open Core.Std
let (|>) x f = f x

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)

open Hitscore_threaded 

let find_contact ~dbh ~verbose first last email =
  let if_verbose fmt = 
    ksprintf (if verbose then print_string else Pervasives.ignore) fmt in
  if_verbose "Person: %S %S %S\n" first last email;
  let by_email = Layout.Search.record_person_by_email ~dbh email in
  begin match by_email with
  | Ok [ one ] ->
    if_verbose
      "  Found one person with that email: %ld.\n"
      one.Layout.Record_person.id;
    (`one one)
  | Ok [] ->
    if_verbose "  Found no person with that email … ";
    begin match Layout.Search.record_person_by_given_name_family_name
        ~dbh first last with
    | Ok [ one ] ->
      if_verbose "BUT found one person with that name: %ld.\n"
        one.Layout.Record_person.id;
      (`to_fix (one,
                sprintf "%S: wrong email for contact %ld" email
                  one.Layout.Record_person.id))
    | Ok [] ->
      if_verbose "neither with that full name … ";
      begin match Layout.Search.record_person_by_nickname_family_name
          ~dbh (Some first) last with
      | Ok [ one ] ->
        if_verbose "BUT found one person with that nick name: %ld.\n"
          one.Layout.Record_person.id;
        (`to_fix (one,
                  sprintf "%S: wrong email for contact %ld" email
                    one.Layout.Record_person.id))
      | Ok [] ->
        if_verbose "neither with that nick-fullname … \n";
        (`none (first, last, email))
      | _ ->
        failwith "search by nick-full-name returned wrong"
      end
    | _ ->
      failwith "search by full-name returned wrong"
    end
  | _ ->
    failwith "search by email returned wrong"
  end

let print = ksprintf
let errbuf = Buffer.create 42
let error fmt = 
  let  f = Buffer.add_string errbuf in
  print f ("ERROR:" ^^ fmt ^^ "\n")
let warning fmt =
  let  f = Buffer.add_string errbuf in
  print f ("WARNING:" ^^ fmt ^^ "\n")
let strlist l = String.concat ~sep:"; " (List.map l (sprintf "%S"))


let find_section sanitized section =
  Array.findi sanitized (function
  | s :: _ when s = section -> true
  | _ -> false) |>
      Option.value_exn_message (sprintf "Can't the find %S section." section)

let list_of_filteri f =
  let rl = ref [] in
  let module M = struct
    exception End
    let lofi () =
      let rec loop c =
        match f c with
        | Some s -> rl := s :: !rl; loop (c + 1)
        | None -> raise End
      in
      try loop 0 with End -> ()
  end in
  M.lofi ();
  List.rev !rl


let int32 ?(msg="") s = try Some (Int32.of_string s) with e ->
  error "%sCannot read an integer from %s" msg s; None


let parse_run_type s =
  match String.split ~on:' ' s with
  | [ "PE"; lengths ] ->
    begin match String.split ~on:'x' lengths with
    | [ left; right ] ->
      Option.bind (int32 left) (fun l -> Some (s, l, int32 right))
    | _ ->
      error "Wrong read length spec (PE): %s" s;
      None
    end
  | [ "SE"; length ] ->
    Option.bind (int32 length) (fun l -> Some (s, l, None))
  | _ ->
    error "Wrong read length spec (PE): %s" s;
    None
    
let parse_date s =
  match String.split ~on:'-' s |> List.map ~f:(String.strip) with
  | [ year; month; day ] ->
    begin try
            Some (ksprintf Layout.Timestamp.of_string
                    "%04s-%02s-%02s 08:00:00-05:00" year month day)
      with Failure e ->
        error "Cannot parse date: %s %s" s e; None
    end
  | l ->
    error "Cannot parse date: %s" s;
    None

 
let col_LibraryID = "LibraryID"
let col_Project_Name = "Project Name"
let col_Concentration__nM_  = "Concentration (nM)"
let col_Note = "Note"
let col_Sample_Name = "Sample Name"
let col_Species___Source  = "Species - Source"
let col_Application = "Application"
let col_Is_Stranded = "Is Stranded"
let col_TruSeq_Control = "TruSeq Control"
let col_RNA_Seq_Control = "RNA Seq Control"
let col_Barcode_Provider = "Barcode Provider"
let col_Barcode_Number = "Barcode Number"
let col_Custom_Barcode_Sequence = "Custom Barcode Sequence"
let col_Custom_Barcode_Location = "Custom Barcode Location"
let col_P5_Adapter_Length  = "P5 Adapter Length"
let col_P7_Adapter_Length  = "P7 Adapter Length"
let col_Bioanalyzer___Well_Number  = "Bioanalyzer - Well Number"
let col_Bioanalyzer___Mean_Fragment_Size  = "Bioanalyzer - Mean Fragment Size"
let col_Bioanalyzer___Min_Fragment_Size  = "Bioanalyzer - Min Fragment Size"
let col_Bioanalyzer___Max_Fragment_Size  = "Bioanalyzer - Max Fragment Size"
let col_Bioanalyzer___PDF = "Bioanalyzer - PDF"
let col_Bioanalyzer___XAD = "Bioanalyzer - XAD"
let col_Agarose_Gel___Well_Number = "Agarose Gel - Well Number"
let col_Agarose_Gel___Mean_Fragment_Size  = "Agarose Gel - Mean Fragment Size"
let col_Agarose_Gel___Min_Fragment_Size  = "Agarose Gel - Min Fragment Size"
let col_Agarose_Gel___Max_Fragment_Size  = "Agarose Gel - Max Fragment Size"
let col_Agarose_Gel___Image_File = "Agarose Gel - Image File"
let col_Protocol_Name = "Protocol Name"
let col_Protocol_File = "Protocol File"
let col_Library_Preparator_Email = "Library Preparator Email"
let col_Notes = "Notes"



let parse ?(dry_run=true) ?(verbose=false) hsc file =
  let if_verbose fmt = 
    ksprintf (if verbose then print_string else Pervasives.ignore) fmt in

  Buffer.clear errbuf;
  printf "========= Loading %S =========\n" file;
  match db_connect hsc with
  | Ok dbh ->
    let loaded = Csv.load ~separator:',' file |> Array.of_list in
    let sanitized =
      let sanitize s =
        (String.split_on_chars ~on:[' '; '\t'; '\n'; '\r'] s) |>
            List.filter ~f:((<>) "") |>
                String.concat ~sep:" "
      in
      Array.map loaded ~f:(List.map ~f:sanitize)
    in
    let stropt s = if s = "" then None else Some s in
    let contacts =
      let contacts_section = find_section sanitized "Contacts" in
      list_of_filteri (fun i ->
        let row = sanitized.(contacts_section + i + 1) in
        if List.hd_exn row = "" && List.length row > 1 then
          begin match row with
          | "" :: [] | [] -> failwith "should not be trying this... (contacts)"
          | ["" ; email ] ->
            begin match Layout.Search.record_person_by_email ~dbh email with
            | Ok [ one ] ->
              if_verbose
                "  Found one person with %S as email: %ld.\n"
                email one.Layout.Record_person.id;
              Some (email, Some one, [])
            | Ok [] ->
              error "  Found no one with %S as email." email;
              None
            | _ ->
              failwithf "Database problem while looking for %S" email ()
            end
          | "" :: email :: first :: middle :: last :: _ as l ->
            begin match (find_contact ~dbh ~verbose first last email) with
            | `one o ->
              Some (email, Some o, [])
            | `to_fix (o, msg) ->
              error "Contact to fix: %s" msg;
              Some (email, None, (List.map (List.tl_exn l) stropt))
            | `none _ -> 
              Some (email, None, (List.map (List.tl_exn l) stropt))
            end
          | l -> 
            error "Wrong contact line: %s" (strlist l); None
          end
        else None)
    in
    if_verbose "Contacts:\n";
    List.iter contacts (function
    | email, None, _ -> if_verbose "  %s (TO CREATE)\n" email
    | email, Some o, _ -> if_verbose "  %s -- %ld\n" email o.Layout.Record_person.id);
    let submission_date, run_type_parsed, invoicing =
      let section = find_section sanitized "Invoicing" in
      let submission_date, run_type_parsed =
        match sanitized.(section) with
        | "Invoicing" :: "Submission Date:" :: date
          :: "Run Type Requested:" :: run_type_str :: _ ->
          (parse_date date, parse_run_type run_type_str)
        | l ->
          error "Wrong invoicing row: %s\n" (strlist l);
          (None, None)
      in
      (submission_date, run_type_parsed,
       list_of_filteri (fun i ->
         let row = sanitized.(section + i + 2) in
         if_verbose "Row: %s\n" (strlist row);
         match row with
         | [] -> None
         | p :: _ when String.is_prefix p ~prefix:"Pool" -> None
         | piemail :: percent :: chartstuff when 
             List.length (String.split ~on:'@' piemail) = 2 ->
           begin match try Some (Float.of_string percent) with _ -> None with
           | Some p -> Some (piemail, p, chartstuff)
           | None -> error "Wrong chartfield row: %s" (strlist row); None
           end
         | l -> error "Wrong chartfield row: %s" (strlist row); None))
    in
    let sum = List.fold_left invoicing ~f:(fun x (_, p, _) -> x +. p) ~init:0. in
    if 99. < sum && sum < 101. then
      ()
    else
      error "Invoicing sums up to %f" sum;
    if_verbose "Invoicing:\n";
    List.iter invoicing (fun (email, p, chartstuff) -> 
      if_verbose "  %s, %f, [%s]\n" email p (strlist chartstuff)
    );
    let pools =
      let sections =
        Array.mapi sanitized ~f:(fun i a -> (i, a)) |> Array.to_list |>
            List.find_all ~f:(function
            | (_, head :: _) when String.is_prefix ~prefix:"Pool" head -> true
            | _ -> false) |>
                List.map ~f:(fun (i, a) -> i)
      in
      List.map sections (fun section ->
        match sanitized.(section) with
        | pool :: "Seeding Concentration (10 - 20 pM):" :: scs
          :: "Total Volume (uL):" :: tvs :: "Concentration (nM):" :: cs :: [] ->
          let seeding_pM, tot_vol, nm =
            let i32 = int32 ~msg:(sprintf "%s row: " pool) in
            (i32 scs, i32 tvs, i32 cs) in
          let pool_libs =
            list_of_filteri (fun i ->
              let row = sanitized.(section + i + 2) in
              match row with
              | [ lib ; percent ] ->
                begin match (int32 percent) with
                | Some p -> Some (lib, p)
                | None -> 
                  error "Wrong pool percentage %s in row [%s]" percent (strlist row);
                  None
                end
              | "" :: [] | [] ->  None
              | _ ->
                error "Wrong pool percentage row: [%s]" (strlist row);
                None) in
          Some (pool, seeding_pM, tot_vol, nm, pool_libs)
        | l ->
          error "Wrong Pool row: %s" (strlist l); None)
    in
    List.iter pools (function
    | Some (pool, _,_,_, l) ->
      let sum = List.fold_left l ~f:(fun x (_, p) -> Int32.(x + p)) ~init:0l in
      if sum < 99l || sum > 100l then
        error "Pooled percentages for %s do not sum up to 100" pool
    | None -> ());

    if_verbose "Pools:\n";
    List.iter pools (function
    | Some (pool, spm, tv, nm, l) -> 
      let f = Option.value_map ~default:"WRONG" ~f:(sprintf "%ld") in
      if_verbose "  %s %s %s %s [%s]\n" pool (f spm) (f tv) (f nm)
        (String.concat ~sep:", " 
           (List.map l ~f:(fun (x,y) -> sprintf "%s:%ld%%" x y)))
    | None -> ()
    );


    let libraries =
      let check_existing_library libname rest =
        let project =
          match rest with
          | "" :: _ | [] -> None
          | p :: _ -> Some p in
        let search = (* working around strangety of PGOCaml (c.f. 
                        email sent to the mailinglist)  *)
          match project with
          | Some _ ->
            Layout.Search.record_stock_library_by_name_project
              ~dbh libname project
          | None ->
            Layout.Search.record_stock_library_by_name ~dbh libname
        in
        begin match search with
            | Ok [one] ->
              if_verbose "Ok found that library: %ld\n"
                one.Layout.Record_stock_library.id;
              Some one
            | Ok [] ->
              error "Library %s is declared as already defined but was not found%s"
                libname 
                (Option.value_map project ~default:"" ~f:(sprintf " in project %s"));
              None
            | Ok l ->
              error "Library %s has an ambiguous name: %d homonyms%s"
                libname (List.length l)
                (Option.value_map project ~default:"" ~f:(sprintf " in project %s"));
              None
            | Error _ ->
              failwith "check_existing_library DB error"
        end
      in


      let section = find_section sanitized "Libraries" in
      let column row name =
        Option.(
          List.findi sanitized.(section + 1) ~f:(fun _ s -> s = name)
          >>= fun (i, _) ->
          List.nth row i
          >>= fun s -> if s = "" then None else Some s
        ) in
      let custom_keys =
        list_of_filteri (fun i ->
          Option.(
            List.findi sanitized.(section + 1) ~f:(fun _ s -> s = col_Notes)
            >>= fun (notes, _) ->
            List.nth loaded.(section + 1) (notes + i + 1))) in

      list_of_filteri (fun i ->
        try 
          let row = sanitized.(section + i + 2)  in
          let is_declared_known l =
            List.length l <= 4
            || (let r = ref true in
                List.iteri l ~f:(fun i x -> 
                  if 4 <= i && i < 31 then
                    match List.nth l i with
                    | None | Some "" -> ()
                    | Some s -> r := false
                  else
                    ());
                !r) in
          match row with
          | libname :: rest as l when libname <> "" && is_declared_known l ->
            if_verbose "%s should be already known\n" libname;
            let exisiting = check_existing_library libname rest in
            let int32 = int32 ~msg:(sprintf "Lib %s: " libname) in
            begin match exisiting with
            | Some lib_t ->
              let key_value_list =
                List.map custom_keys 
                  (fun c -> (c, column loaded.(section + i + 2) c)) in
              Some (match rest with
              | [] | [ _ ]
              |  _ :: "" :: []
              |  _ :: "" :: "" :: _ -> 
                `existing (libname, lib_t, None, None, key_value_list)
              |  _ :: conc :: []
              |  _ :: conc :: "" :: _ ->
                `existing (libname, lib_t, int32 conc, None, key_value_list)
              | _ :: "" :: note :: _ ->
                `existing (libname, lib_t, None, Some note, key_value_list)
              | _ :: conc :: note :: _ ->
                `existing (libname, lib_t, int32 conc, Some note, key_value_list))
            | None -> Some (`wrong libname)
            end
          | libname :: rest when libname <> ""->
            if_verbose "%s should be a new one\n" libname;
            let open Option in
            let mandatory row col =
              match column row col with
              | Some s -> Some s
              | None -> 
                error "Lib: %s, mandatory field not provided: %s" libname col; None in
            let mandatory32 r c = 
              mandatory r c >>= int32 ~msg:(sprintf "Lib: %s (%s)" libname c) in
            let column32 r c = 
              column r c >>= int32 ~msg:(sprintf "Lib: %s (%s)" libname c) in
            let int32 = int32 ~msg:(sprintf "Lib %s: " libname) in
            let project = column row col_Project_Name in
            let conc = column32 row col_Concentration__nM_ in
            let note = column row col_Note in
            let sample_name = column row col_Sample_Name in
            let species = column row col_Species___Source in
            let application = mandatory row col_Application in
            let stranded = mandatory row col_Is_Stranded
                           >>| String.lowercase 
                           >>= fun s ->
                           try Some (Bool.of_string s) with _ -> None in
            let truseq_control =
              column row col_TruSeq_Control 
              >>| String.lowercase 
              >>= fun s ->
              try Some (Bool.of_string s) with _ -> None
            in
            let rnaseq_control = column row col_RNA_Seq_Control in
            let barcode_type =
              column row col_Barcode_Provider
              >>| String.lowercase
              |! function
                | Some s ->
                  begin match Layout.Enumeration_barcode_provider.of_string s with
                  | Ok s -> s
                  | Error s ->
                    error "Lib: %s cannot recognize barcode provider: %s" libname s;
                    `none
                  end
                | None -> `none in
            let barcodes =
              match column row col_Barcode_Number with
              | None -> []
              | Some s ->
                String.split ~on:',' s 
                |! List.map ~f:String.strip
                |! List.find_all ~f:((<>) "") 
                |! List.filter_map ~f:int32
            in
            let custom_barcode_sequence =
              (column row col_Custom_Barcode_Sequence 
               >>= fun s -> 
               return (String.split ~on:',' s))
              |! value_map ~default:[] ~f:(List.map ~f:String.strip)
            in
            let custom_barcode_positions =
              (column row col_Custom_Barcode_Location
               >>= fun s ->
               return (String.split ~on:',' (String.lowercase s)))
              |! value_map ~default:[] ~f:(List.map ~f:String.strip)
              |! List.filter_map ~f:(fun s ->
                match String.split ~on:':' s |! List.map ~f:String.strip with
                | [ "r1" ; two ] -> int32 two >>= fun i -> Some (`on_r1 i) 
                | [ "r2" ; two ] -> int32 two >>= fun i -> Some (`on_r2 i) 
                | [ "i"  ; two ] -> int32 two >>= fun i -> Some (`on_i i ) 
                | _ ->
                  error "Lib %s: cannot understand custom-barcode-loc: %S" libname s;
                  None)
            in
            let rawrow = loaded.(section + i + 2) in
            let p5_adapter_length = mandatory32 row col_P5_Adapter_Length in
            let p7_adapter_length = mandatory32 row col_P7_Adapter_Length in
            let bio_wnb  = mandatory32 row col_Bioanalyzer___Well_Number in
            let bio_avg  = mandatory32 row col_Bioanalyzer___Mean_Fragment_Size in
            let bio_min  = mandatory32 row col_Bioanalyzer___Min_Fragment_Size in
            let bio_max  = mandatory32 row col_Bioanalyzer___Max_Fragment_Size in
            let bio_pdf  = mandatory rawrow col_Bioanalyzer___PDF in
            let bio_xad  = mandatory rawrow col_Bioanalyzer___XAD in
            let arg_wnb  = column32 row col_Agarose_Gel___Well_Number in
            let arg_avg  = column32 row col_Agarose_Gel___Mean_Fragment_Size in
            let arg_min  = column32 row col_Agarose_Gel___Min_Fragment_Size in
            let arg_max  = column32 row col_Agarose_Gel___Max_Fragment_Size in
            let arg_img  = column rawrow col_Agarose_Gel___Image_File in
            let protocol_name  = mandatory row col_Protocol_Name in
            let protocol_file  = mandatory rawrow col_Protocol_File in
            let preparator     = mandatory row col_Library_Preparator_Email in
            let notes          = column rawrow col_Notes in
            let key_value_list =
              List.map custom_keys (fun c -> (c, column rawrow c)) in
            Some (`new_lib (libname, project, conc, note,
                            sample_name, species,
                            application, stranded,
                            truseq_control, rnaseq_control,
                            barcode_type, barcodes, 
                            custom_barcode_sequence, custom_barcode_positions,
                            p5_adapter_length, p7_adapter_length,
                            bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                            arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                            protocol_name , 
                            protocol_file , 
                            preparator    , 
                            notes         , 
                            key_value_list))
          | _ ->  None
        with
        | Invalid_argument "index out of bounds" -> None
        | e -> warning "Saw exception: %s" (Exn.to_string e); None)
    in
    if_verbose "Libraries:\n";
    List.iter libraries (function
    | `wrong libname -> if_verbose "  wrong lib: %s\n" libname
    | `existing (s, _, c, n, kv) -> 
      if_verbose "  existing lib: %s%s%s {%s}\n" s
        (Option.value_map c ~default:" (no concentration)" ~f:(sprintf " (%ld nM)"))
        (Option.value_map n ~default:" (no note)" ~f:(sprintf " (note: %S)"))
        (String.concat ~sep:", " 
           (List.map kv ~f:(function
           | (k, Some v) -> sprintf "[%S -- %S]" k v
           | (k, None)   -> sprintf "[NO %S]" k)))

    | `new_lib (s, project, conc, note,
                sample_name, species, app, strd, tsc, rsc,
                bt, bcs, cbs, cbp,
                p5, p7,
                bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                protocol_name , 
                protocol_file , 
                preparator    , 
                notes         , 
                key_value_list) -> 
      let vm = Option.value_map in
      let nl = "\n" in
      let bioarg (bio_wnb, bio_avg, bio_min, bio_max) =
        (String.concat ~sep:", " [
          vm bio_wnb ~default:"[NO WNB]" ~f:(sprintf "[wnb: %ld]");  
          vm bio_avg ~default:"[NO AVG]" ~f:(sprintf "[avg: %ld]");  
          vm bio_min ~default:"[NO MIN]" ~f:(sprintf "[min: %ld]");  
          vm bio_max ~default:"[NO MAX]" ~f:(sprintf "[max: %ld]"); 
        ]) in
      if_verbose "  new lib: %s\n    %s\n" s
        (String.concat ~sep:"    " ([
          vm project ~default:"[no project]" ~f:(sprintf "[Proj: %s]");
          vm sample_name ~default:"[no sample]" ~f:(sprintf "[Sample: %s]");
          vm species ~default:"[no species]" ~f:(sprintf "[Org: %s]"); nl;
          vm app ~default:"[NO APP]" ~f:(sprintf "[App: %s]");
          vm strd ~default:"[NO STRANDEDNESS]"
            ~f:(fun b -> if b then "[stranded]" else "[not stranded]"); nl;
          vm tsc ~default:"[no truseq-c]" ~f:(sprintf "[TruSeqC: %b]");
          vm rsc ~default:"[no rnaseq-c]" ~f:(sprintf "[RNASeqC: %s]"); nl;
          sprintf "Barcode [Type: %s"
            (Layout.Enumeration_barcode_provider.to_string bt);
          sprintf "(%s)]" (String.concat ~sep:", " 
                                      (List.map bcs (Int32.to_string)));
          if cbs <> [] then
            sprintf "[Custom: (%s)]" (String.concat ~sep:", " cbs)
          else "";
          if cbp <> [] then
            sprintf "[Position: %s]"
              (String.concat ~sep:", " (List.map cbp ~f:(function
              | `on_r1 i -> sprintf "%ld on R1" i
              | `on_r2 i -> sprintf "%ld on R2" i
              | `on_i i -> sprintf "%ld on Idx" i))) else ""; nl;
          vm p5 ~default:"[NO P5 LENGTH]" ~f:(sprintf "[P5: %ld]");
          vm p7 ~default:"[NO P7 LENGTH]" ~f:(sprintf "[P7: %ld]"); nl;
          sprintf "[Bioanalzer: %s]" (bioarg (bio_wnb, bio_avg, bio_min, bio_max));
          vm bio_pdf ~default:"[NO PDF]" ~f:(sprintf "\n      [pdf: %S]");
          vm bio_xad ~default:"[NO XAD]" ~f:(sprintf "\n      [xad: %S]"); nl;
          sprintf "[Agarose Gel %s]" (bioarg (arg_wnb, arg_avg, arg_min, arg_max));
          vm arg_img ~default:"[NO IMG]" ~f:(sprintf "\n      [img: %S]"); nl;
          vm protocol_name  ~default:"[NO PROTOCOL]" ~f:(sprintf "[Protocol: %s]"); 
          vm protocol_file  ~default:"[NO P-FILE]" ~f:(sprintf "[P-File: %s]"); nl; 
          vm preparator     ~default:"[NO PREPARATOR]" ~f:(sprintf "[Prep by %s]"); nl; 
          vm notes          ~default:"" ~f:(sprintf "[Note: %S]");
         ]
         @ 
           (List.map key_value_list ~f:(function
           | (k, Some v) -> sprintf "\n    [%S -- %S]" k v
           | (k, None)   -> sprintf "\n    [NO %S]" k))
         )
        )
    );

    (* More checks *)
    let () =
      let assoc_sample_org =
        List.filter_map libraries ~f:(function
        | `new_lib (s, project, conc, note,
                    sample_name, species, app, strd, tsc, rsc,
                    bt, bcs, cbs, cbp,
                    p5, p7,
                    bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                    arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                    protocol_name, protocol_file, preparator, notes, 
                    key_value_list) ->
          Some (sprintf "%s.%S"
                  (Option.value ~default:"_" project)
                  (Option.value ~default:"" sample_name), species)
        | _ -> None)
      in
      let l1 = List.dedup assoc_sample_org |> List.sort ~cmp:compare in
      let l2 =
        let compare  = (fun a b -> compare (fst a) (fst b)) in
        List.dedup assoc_sample_org ~compare |> List.sort ~cmp:compare in
      if l1 <> l2 then
        List.iter l2 (fun (s, h) ->
          match List.find_all l1 ~f:(fun (x, _) -> x = s) with
          | [ one ] -> ()
          | [] -> failwith "should nt be there"
          | more ->
            error "Sample %s is defined with too many organisms: %s" s
              (String.concat ~sep:", " 
                 (List.map ~f:snd
                    (List.Assoc.map more (Option.value ~default:"NONE")))));
    in

    (* Dry or Wet additions to the database *)
    let fake_pointer = ref 10000 in
    let dry_buffer = ref [] in
    let print_dry_buffer () =
      if dry_run then
        printf "=== Dry-run: ===\n"
      else
        printf "=== WET-RUN !!! ===\n";
      List.iter (List.rev !dry_buffer) (fun (p, l) ->
        if p > 0l then
          printf " * [%ld] %s\n" p l
        else
          printf " * %s\n" l
      ) in
    let run ~dbh ~fake ~real ~log =
      if dry_run then (
        match List.find !dry_buffer (fun (p, l) -> l = log) with
        | Some (p, _) -> Ok (fake p)
        | None ->
          incr fake_pointer;
          let pointer = (Int32.of_int !fake_pointer |> Option.value_exn) in
          dry_buffer := (pointer, log) :: !dry_buffer;
          Ok (fake pointer)
      ) else (
        dry_buffer := (0l, log) :: !dry_buffer;
        Result.(
          real dbh
          >>= fun real_thing ->
          Layout.Record_log.add_value ~dbh ~log
          >>= fun _ ->
          return real_thing))
    in
    let contacts =
      List.map contacts (function
      | email, None, contents ->
        let get nth name = 
          match List.nth contents nth with
          | Some (Some s) -> s
          | None | Some None -> 
            error "Contact %S: %s not provided" email name; 
            (name ^ "-fake")
        in
        let given_name = get 1 "Given-name" in
        let middle_name = List.nth contents 2 |! Option.value ~default:None in
        let family_name = get 3 "Family-name" in
        let nickname = List.nth contents 4  |! Option.value ~default:None in
        let login = List.nth contents 5  |! Option.value ~default:None in
        let id =
          run ~dbh
            ~fake:(fun x -> { Layout.Record_person.id = x })
            ~real:(fun dbh ->
              Layout.Record_person.add_value ~dbh
                ~email ~given_name ?middle_name ~family_name
                ?nickname ?login ?print_name:None ?note:None)
            ~log:(sprintf "(add_person %s)" email)
        in
        (email, id |! Result.ok)
      | email, Some o, _ -> (email, Some o))
    in

    let bio_directories = ref [] in
    let arg_directories = ref [] in
    let stock = ref [] in
    List.iter libraries (function
    | `wrong libname -> ()
    | `existing _ ->  (* TODO *) ()    
    | `new_lib (libname, project, conc, note,
                sample_name, species, app, strd, tsc, rsc,
                bt, bcs, cbs, cbp,
                p5, p7,
                bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                protocol_name , 
                protocol_file , 
                preparator    , 
                notes         , 
                key_value_list) ->
      let open Option in
      let protocol =
        match protocol_name with
        | Some name ->
          begin match Layout.Search.record_protocol_by_name ~dbh name with
          | Ok [] ->
            begin match protocol_file with
            | Some s ->
              let hr_tag = "HRTAG_TO_IMPROVE" in
              run ~dbh
                ~fake:(fun x -> { Layout.File_system.id = x })
                  ~real:(fun dbh ->
                    Layout.File_system.add_volume ~dbh 
                      ~kind:`protocol_directory
                      ~hr_tag
                      ~files:[ Layout.File_system.Tree.file s ])
                ~log:(sprintf 
                        "(add_volume protocol_directory %s (files %S))" hr_tag s)
            | None ->
              error "Lib: %s, Protocol %S is not in the DB and has no file." 
                libname name;
              Ok { Layout.File_system.id = -1l }
            end
            |! (function
              | Ok doc ->
                run ~dbh
                  ~fake:(fun x -> { Layout.Record_protocol.id = x})
                  ~real:(fun dbh ->
                    Layout.Record_protocol.add_value ~dbh
                      ~name ~doc ?note:None)
                  ~log:(sprintf "(add_protocol %S %ld)" name doc.Layout.File_system.id)
                |! Result.ok
              | _ ->
                failwith "DB Error add_protocol")
          | Ok [one] -> Some one
          | _ -> failwith "DB Error (record_protocol_by_name)"
          end
        | None -> None
      in
      let organism = 
        Option.bind species (fun name ->
          match Layout.Search.record_organism_by_name ~dbh (Some name) with
          | Ok [] ->
            run ~dbh
              ~fake:(fun x -> { Layout.Record_organism.id = x })
              ~real:(fun dbh ->
                Layout.Record_organism.add_value ~dbh 
                  ~name ?informal:None ?note:None)
              ~log:(sprintf "(add_organism %S)" name) |! Result.ok
          | Ok [one] -> Some one
          | _ -> failwith "DB error (record_organism_by_name)")
      in
      let sample =
        let open Option in
        bind sample_name (fun name ->
          let search =          
            if is_some project then
              Layout.Search.record_sample_by_name_project ~dbh name project
            else
              Layout.Search.record_sample_by_name ~dbh name in
          match search with
          | Ok [] ->
            Layout.Record_sample.(
              run ~dbh ~fake:(fun x -> { id = x })
                ~real:(fun dbh ->
                  add_value ~dbh 
                    ~name ?organism ?project ?note:None)
                ~log:(sprintf "(add_organism %S%s%s)" name
                        (value_map organism ~default:""
                           ~f:(fun { Layout.Record_organism.id} ->
                             sprintf " %ld" id))
                        (value_map project ~default:"" ~f:(sprintf " %S"))) 
               |! Result.ok)
          | Ok [one] -> Some one
          | _ -> failwith "DB error search samples")
      in
      let stock_lib =
        let name = libname in
        let stranded = value ~default:false strd in
        let truseq_control = value ~default:false tsc in
        let custom_barcodes =
          Array.of_list cbs 
          |! Array.filter_map ~f:(fun sequence ->
            let position_in_r1 =
              List.find_map cbp (function | `on_r1 i -> Some i | _ -> None) in
            let position_in_r2 =
              List.find_map cbp (function | `on_r2 i -> Some i | _ -> None) in
            let position_in_index =
              List.find_map cbp (function | `on_i i -> Some i | _ -> None) in
            Layout.Record_custom_barcode.(
              run ~dbh ~fake:(fun x -> { id = x })
                ~real:(fun dbh ->
                  add_value ~dbh
                    ?position_in_r1 ?position_in_r2 ?position_in_index ~sequence)
                ~log:(sprintf "(add_custom_barcode %s (r1 %s) (r2 %s) (index %s))"
                        sequence
                        (value_map position_in_r1 ~default:"None" ~f:(sprintf "%ld")) 
                        (value_map position_in_r2 ~default:"None" ~f:(sprintf "%ld")) 
                        (value_map position_in_index ~default:"None" ~f:(sprintf "%ld")))
            ) |! Result.ok
          )
        in
        let preparator =
          match preparator with
          | None -> None
          | Some email ->
            begin match List.Assoc.find contacts email with
            | Some (Some id) -> Some id
            | Some None -> 
              warning "Lib %s: found 'None' contact for %S, \
                        this should be already reported..." libname email;
              None
            | None ->
              begin match Layout.Search.record_person_by_email ~dbh email with
              | Ok [] ->
                error "Lib %s: Unknown preparator: %s" libname email;
                None
              | Ok [one] -> Some one
              | _ -> failwith "DB error searching preparator"
              end
            end
        in
        Layout.Record_stock_library.(
          run ~dbh ~fake:(fun x -> { id = x })
            ~real:(fun dbh ->
              add_value ~dbh 
                ~name ?project 
                ?sample ?protocol ?application:app ~stranded
                ~truseq_control ?rnaseq_control:rsc
                ~barcode_type:bt ~barcodes:(Array.of_list bcs)
                ~custom_barcodes
                ?p5_adapter_length:p5
                ?p7_adapter_length:p7
                ?preparator
                ?note:None
            )
            ~log:(sprintf "(add_stock_library %s%s %s %s %s)" 
                    (value_map project ~default:"" ~f:(sprintf "%s."))
                    name
                    (value_map sample ~default:"no_sample"
                       ~f:(fun { Layout.Record_sample.id} ->
                         sprintf " (sample %ld)" id))
                    (value_map protocol ~default:"no_protocol"
                       ~f:(fun { Layout.Record_protocol.id} ->
                         sprintf " (protocol %ld)" id))
                    (value_map preparator ~default:"no_preparator"
                       ~f:(fun { Layout.Record_person.id} ->
                         sprintf " (preparator %ld)" id))
            ))
      in
      let bioanalyzer =
        let dir = 
          match bio_pdf, bio_xad with
          | Some pdf, Some xad -> [pdf; xad]
          | _ -> []
        in
        let files =
          match List.find !bio_directories (fun (l, i) -> l = dir) with
          | Some (_, i) -> i
          | None ->
            let hr_tag = sprintf "xad_pdf_%s" libname in
            run ~dbh
              ~fake:(fun x -> { Layout.File_system.id = x })
              ~real:(fun dbh ->
                Layout.File_system.add_volume ~dbh 
                  ~kind:`bioanalyzer_directory
                  ~hr_tag
                  ~files:(List.map dir ~f:(Layout.File_system.Tree.file)))
              ~log:(sprintf 
                      "(add_volume bioanalyzer_directory %s (files (%s)))"
                      hr_tag (String.concat ~sep:" " (List.map dir (sprintf "%S"))))
            |! Result.ok
            |! fun x ->
              bio_directories := (dir, x) :: !bio_directories;
              x
        in
        stock_lib |! Result.ok
        >>= fun library ->
        let f32o o = bind o (fun x -> return (Float.of_int64 (Int64.of_int32 x))) in
        Layout.Record_bioanalyzer.(
          run ~dbh ~fake:(fun x -> { id = x })
            ~real:(fun dbh ->
              add_value ~dbh ~library
                ?well_number:bio_wnb
                ?mean_fragment_size:(f32o bio_avg) 
                ?min_fragment_size: (f32o bio_min) 
                ?max_fragment_size: (f32o bio_max) 
                ?note:None
                ?files)
            ~log:(sprintf "(add_bioanalyzer %ld%s%s)" 
                    (library.Layout.Record_stock_library.id)
                    (value_map bio_wnb ~default:"" ~f:(sprintf " (well_number %ld)"))
                    (value_map files ~default:""
                       ~f:(fun { Layout.File_system.id } -> sprintf " (files %ld)" id))
            )) |! Result.ok
      in
      let agarose_gel () =
        let dir = 
          match arg_img with
          | Some img -> [img]
          | _ -> []
        in
        let files =
          match List.find !arg_directories (fun (l, i) -> l = dir) with
          | Some (_, i) -> i
          | None ->
            let hr_tag = sprintf "img_%s" libname in
            run ~dbh
              ~fake:(fun x -> { Layout.File_system.id = x })
              ~real:(fun dbh ->
                Layout.File_system.add_volume ~dbh 
                  ~kind:`agarose_gel_directory
                  ~hr_tag
                  ~files:(List.map dir ~f:(Layout.File_system.Tree.file)))
              ~log:(sprintf 
                      "(add_volume agarose_gel_directory %s (files (%s)))"
                      hr_tag (String.concat ~sep:" " (List.map dir (sprintf "%S"))))
            |! Result.ok
            |! fun x ->
              arg_directories := (dir, x) :: !arg_directories;
              x
        in
        stock_lib |! Result.ok
        >>= fun library ->
        let f32o o = bind o (fun x -> return (Float.of_int64 (Int64.of_int32 x))) in
        Layout.Record_agarose_gel.(
          run ~dbh ~fake:(fun x -> { id = x })
            ~real:(fun dbh ->
              add_value ~dbh ~library
                ?well_number:arg_wnb
                ?mean_fragment_size:(f32o arg_avg) 
                ?min_fragment_size: (f32o arg_min) 
                ?max_fragment_size: (f32o arg_max) 
                ?note:None
                ?files)
            ~log:(sprintf "(add_agarose_gel %ld%s%s)" 
                    (library.Layout.Record_stock_library.id)
                    (value_map arg_wnb ~default:"" ~f:(sprintf " (well_number %ld)"))
                    (value_map files ~default:""
                       ~f:(fun { Layout.File_system.id } -> sprintf " (files %ld)" id))
            )) |! Result.ok
      in
      begin match arg_wnb, arg_avg, arg_min, arg_max, arg_img with
      | Some _, Some _, Some _, Some _, Some _ -> agarose_gel () |! Pervasives.ignore
      | None, None, None, None, None -> ()
      | _ ->
        error "Lib %s: Incomplete Agarose Gel" libname;
        agarose_gel () |! Pervasives.ignore
      end;
      Pervasives.ignore (bioanalyzer, agarose_gel);
      stock := (libname, project, 
                stock_lib |! Result.ok_exn ~fail:(Failure "no stock lib!")) :: !stock;
      ());

    let lanes = List.map pools (function
      | Some (pool, spm, tv, nm, input_libs) ->
        let open Option in
        let libraries =
          List.map input_libs (fun (libname, percent) ->
            let the_lib, concentration, note, key_values =
              if libname = "PhiX" then
                Layout.Search.record_stock_library_by_name ~dbh "PhiX_v3" 
                |! Result.ok_exn ~fail:(Failure "Can't find PhiX_v3 !!!")
                |! fun lib ->
                  (List.hd_exn lib, None, None, [])
              else
                match List.find_map libraries (function
                | `wrong libname -> None
                | `existing (ln, lib_t, conc, note, kv) ->
                  if libname = ln then Some (lib_t, conc, note, kv) else None
                | `new_lib (ln, project, conc, note,
                            sample_name, species, app, strd, tsc, rsc,
                            bt, bcs, cbs, cbp,
                            p5, p7,
                          bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                            arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                            protocol_name , 
                            protocol_file , 
                            preparator    , 
                            notes         , 
                            key_value_list) ->
                  if ln = libname then
                    List.find_map !stock (fun (n, _, s) -> 
                      if n = libname then Some (s, conc, note, key_value_list) 
                      else None)
                  else
                    None
                ) with
                | None ->
                  error "Pool: %s, can't find library %S" pool libname;
                  ({Layout.Record_stock_library. id = 99l}, None,
                   Some (sprintf "Completely fake stock library: %s for pool %s"
                           libname pool), [])
                | Some (s, conc, note, kv) -> (s, conc, note, kv)
            in
            let user_db =
              List.filter_map key_values (function
              | k, None -> None
              | key, Some value ->
                Layout.Record_key_value.(
                  run ~dbh  ~fake:(fun x -> { id = x })
                    ~real:(fun dbh ->
                      add_value ~dbh ~key ~value)
                    ~log:(sprintf "(add_key_value %S %S)" key value)) |! Result.ok)
              |! Array.of_list in
            let input_lib =
              let concentration_nM =
                concentration >>| Int64.of_int32 >>| Float.of_int64 in
              Layout.Record_input_library.(
                run ~dbh ~fake:(fun x -> { id = x })
                  ~real:(fun dbh ->
                    add_value ~dbh ~library:the_lib
                      ~submission_date:(value submission_date ~default:(Time.now ()))
                      ?volume_uL:None ?concentration_nM ~user_db ?note)
                ~log:(sprintf "(add_input_library %ld (submission_date %S)%s \
                                (user_db (%s))%s)" 
                        (the_lib.Layout.Record_stock_library.id)
                        (value_map ~f:Time.to_string ~default:"NONE" submission_date)
                        (value_map concentration_nM
                           ~default:"" ~f:(sprintf " (concentration_nM %f)"))
                        (String.concat ~sep:" " 
                           (List.map (Array.to_list user_db) ~f:(fun l ->
                             sprintf "%ld" l.Layout.Record_key_value.id)))
                        (value_map note ~default:"" ~f:(sprintf " (note %S)"))
                )) |! Result.ok
            in
            input_lib)  |! List.filter_map ~f:(fun x -> x) |! Array.of_list in
        let pooled_percentages = List.map input_libs ~f:snd |! Array.of_list in
        let requested_read_length_1, requested_read_length_2 =
          match run_type_parsed with Some (_, l, r) -> (l, r) | None -> (42l, None) in
        let contacts = List.filter_map contacts snd |! Array.of_list in
        Layout.Record_lane.(
          run ~dbh ~fake:(fun x -> { id = x })
            ~real:(fun dbh ->
              add_value ~dbh ~libraries ~pooled_percentages
                ?seeding_concentration_pM:(spm >>| Int64.of_int32 >>| Float.of_int64)
                ?total_volume:(tv >>| Int64.of_int32 >>| Float.of_int64)
                ~requested_read_length_1 ?requested_read_length_2
                ~contacts)
            ~log:(sprintf "(add_lane (libraries (%s)) (percentages (%s)) \
                          (contacts (%s)))"
                    (String.concat ~sep:" " 
                       (List.map (Array.to_list libraries) ~f:(fun l ->
                         sprintf "%ld" l.Layout.Record_input_library.id)))
                    (String.concat ~sep:" "
                       (List.map (Array.to_list pooled_percentages) 
                          ~f:(sprintf "%ld")))
                    (String.concat ~sep:" " 
                       (List.map (Array.to_list contacts) ~f:(fun l ->
                         sprintf "%ld" l.Layout.Record_person.id)))
            )) |! Result.ok
      | None -> None
    ) |! List.filter_map ~f:(fun x -> x) |! Array.of_list in
    let invoices =
      let open Option in
      List.map invoicing (fun (piemail, p, chartstuff) ->
        let pi =
          begin match List.Assoc.find contacts piemail with
          | Some (Some id) -> Some id
          | Some None -> 
            warning "Invoice for %s: found 'None' contact for that email, \
                        this should be already reported..." piemail;
            None
          | None ->
            begin match Layout.Search.record_person_by_email ~dbh piemail with
            | Ok [] ->
              error "Invoice for %s: Unknown PI." piemail;
              None
            | Ok [one] -> Some one
            | _ -> failwith "DB error searching preparator"
            end
          end
          |! value ~default:{ Layout.Record_person.id = 42004200l } in
        let account_number, fund, org, program, project =
          let mandatory msg s = 
            if s = "" then (
              error "Invoicing for %s: wrong %s: %s (mandatory field)" piemail msg s;
              None) else Some s in
          let optional s = if s = "" then None else Some s in
          match chartstuff with
          | an :: f :: o :: prog :: proj :: [] ->
            (mandatory "Account number" an,
             mandatory "Fund" f,
             mandatory "Org" o,
             optional prog,
             mandatory "Project" proj)
          | _ -> 
            error "Wrong chartfield for invoice for %s: %s" piemail
              (strlist chartstuff);
            None, None, None, None, None 
        in
        Layout.Record_invoicing.(
          run ~dbh ~fake:(fun x -> { id = x })
            ~real:(fun dbh ->
              add_value ~dbh ~pi 
                ~percentage:p
                ~lanes
                ?account_number ?fund ?org ?program ?project
                ?note:None)
            ~log:(sprintf "(add_invoicing (pi %ld) (percentage %g) \
                            (lanes (%s))%s%s%s%s%s)"
                    pi.Layout.Record_person.id p
                    (String.concat ~sep:" " 
                       (List.map (Array.to_list lanes) ~f:(fun l ->
                         sprintf "%ld" l.Layout.Record_lane.id)))
                    (value_map account_number ~default:""
                       ~f:(sprintf " (account_number %s)"))
                    (value_map fund ~default:"" ~f:(sprintf " (fund %s)"))
                    (value_map org ~default:"" ~f:(sprintf " (org %s)"))
                    (value_map program ~default:"" ~f:(sprintf " (program %s)"))
                    (value_map project ~default:"" ~f:(sprintf " (project %s)"))
            )) |! Result.ok)
    in
    Pervasives.ignore invoices;

    begin match (Buffer.contents errbuf) with
    | "" -> printf "=== No errors or warnings detected. ===\n"
    | s -> printf "=== ERRORS and WARNINGS: ===\n%s\n" s;
    end;
    print_dry_buffer ();
    ()
  | Error (`pg_exn e) ->
    eprintf "Could not connect to the database: %s\n" (Exn.to_string e)
