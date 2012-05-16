open Core.Std
let (|>) x f = f x

open Hitscore_app_util
open Hitscore
open Flow

let find_contact ~dbh ~verbose first last email =
  let if_verbose fmt = 
    ksprintf (if verbose then print_string else Pervasives.ignore) fmt in
  if_verbose "Person: %S %S %S\n" first last email;
  let layout = Classy.make dbh in
  layout#person#all
  >>= fun all_the_people ->
  of_list_sequential all_the_people (fun p ->
    if p#email = email then
      return (Some (`one p#g_pointer))
    else if (p#given_name = first || p#nickname = Some first)
        && p#family_name = last then
      return (Some
                (`to_fix (p#g_pointer,
                          sprintf "%S: wrong email for contact %d" email p#g_id)))
    else
          return None)
  >>| List.filter_opt
  >>= fun filtered ->
  begin match filtered with
  | [ one ] -> return one
  | other -> return (`none (first, last, email))
  end

let print = ksprintf
let errbuf = Buffer.create 42
let perror fmt = 
  let  f = Buffer.add_string errbuf in
  print f ("ERROR:" ^^ fmt ^^ "\n")
let warning fmt =
  let  f = Buffer.add_string errbuf in
  print f ("WARNING:" ^^ fmt ^^ "\n")
let strlist l = String.concat ~sep:"; " (List.map l (sprintf "%S"))


let find_section sanitized section =
  Array.findi sanitized (fun _ -> function
  | s :: _ when s = section -> true
  | _ -> false)
  |! Option.value_exn_message (sprintf "Can't the find %S section." section)
  |! fst

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

let get_section sanitized ~start ~first_column_condition =
  list_of_filteri (fun i ->
    let row = sanitized.(start + i) in
    if not (List.for_all row ((=) ""))
      && first_column_condition (List.hd_exn row) then
      Some row
    else
      None)

let int ?(msg="") s =
  try Scanf.sscanf s "%d" Option.some
  with e ->
    perror "%sCannot read an integer from %s" msg s; None


let parse_run_type s =
  match String.split ~on:' ' s with
  | [ "PE"; lengths ] ->
    begin match String.split ~on:'x' lengths with
    | [ left; right ] ->
      Option.bind (int left) (fun l -> Some (s, l, int right))
    | _ ->
      perror "Wrong read length spec (PE): %s" s;
      None
    end
  | [ "SE"; length ] ->
    Option.bind (int length) (fun l -> Some (s, l, None))
  | _ ->
    perror "Wrong read length spec (PE): %s" s;
    None
    
let parse_date s =
  match String.split ~on:'/' s |> List.map ~f:(String.strip) with
  | [ month; day; year ] ->
    begin try
            Some (ksprintf Time.of_string
                    "%04d-%02d-%02d 08:00:00-05:00"
                    (Int.of_string year) (Int.of_string month)
                    (Int.of_string day))
      with Failure e ->
        perror "Cannot parse date: %s %s" s e; None
    end
  | l ->
    perror "Cannot parse date: %s" s;
    None

 
let col_LibraryID = "LibraryID"
let col_Project_Name = "Project Name"
let col_Concentration__nM_  = "Concentration (nM)"
let col_Note = "Note"
let col_Sample_Name = "Sample Name"
let col_Library_Short_Description = "Library Short Description"
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


let search_person_by_email layout email =
  layout#person#all
  >>| List.filter ~f:(fun p -> p#email = email)
  >>= function
  | [one] -> return one
  | _ -> failwithf "more than one person with email %S" email () 

let stropt s = if s = "" then None else Some s
    
(* parse_contacts -> (email, pointer?, [fields])? *)
let parse_contacts ~verbose ~layout ~dbh sanitized =
  let contact_rows =
    let contacts_section = find_section sanitized "Contacts" in
    get_section sanitized ~start:(contacts_section + 1)
      ~first_column_condition:((<>) "Invoicing") in
  of_list_sequential contact_rows (function
  | "" :: [] | [] -> failwithf "should not be trying this... (contacts)" ()
  | ["" ; email ] ->
    search_person_by_email layout email
    >>= fun p ->
    return (email, Some p#g_pointer, [])
      (* 
         begin match Layout.Search.record_person_by_email ~dbh email with
         | Ok [ one ] ->
         if_verbose
         "  Found one person with %S as email: %d.\n"
         email one.Layout.Record_person.id;
         Some (email, Some one, [])
         | Ok [] ->
         perror "  Found no one with %S as email." email;
         None
         | _ ->
         failwithf "Database problem while looking for %S" email ()
         end *)
  | printname :: email :: first :: middle :: last :: _ as l ->
    find_contact ~dbh ~verbose first last email
    >>= (function
    | `one o ->
      return (email, Some o, [])
    | `to_fix (o, msg) ->
      perror "Contact to fix: %s" msg;
      return (email, None, (List.map l stropt))
    | `none _ -> 
      return (email, None, (List.map l stropt)))
  | l -> 
    failwithf "Wrong contact line: %s" (strlist l) ())
    
let parse_pools ~phix sanitized =
  let sections =
    Array.mapi sanitized ~f:(fun i a -> (i, a)) |> Array.to_list |>
        List.filter ~f:(function
        | (_, head :: _) when String.is_prefix ~prefix:"Pool" head -> true
        | _ -> false) |>
            List.map ~f:(fun (i, a) -> i)
  in
  List.map sections (fun section ->
    match sanitized.(section) with
    | pool :: "Seeding Concentration (10 - 20 pM):" :: scs
      :: "Total Volume (uL):" :: tvs :: "Concentration (nM):" :: cs :: 
        emptyness when List.for_all emptyness ((=) "") ->
      let seeding_pM, tot_vol, nm =
        let i32 = int ~msg:(sprintf "%s row: " pool) in
        (i32 scs, i32 tvs, i32 cs) in
      let pool_libs =
        list_of_filteri (fun i ->
          let row = sanitized.(section + i + 2) in
          let is_empty e = List.for_all e ((=) "") in
          match row with
          | emptyness when is_empty emptyness -> None
          | "Libraries" :: emptyness when is_empty emptyness -> None
          | lib :: percent :: emptyness when List.for_all emptyness ((=) "") ->
            begin match try Some (Float.of_string percent) with e -> None with
            | Some p -> Some (lib, p)
            | None -> 
              perror "Wrong pool percentage %s in row [%s]" percent (strlist row);
              None
            end
          | _ -> (* We should had hit another pool *) None) in
      let pool_libs_redistributed =
        match List.Assoc.find phix pool with
        | None -> pool_libs
        | Some p ->
          let libnb = List.length pool_libs |! float in
          ("PhiX", float_of_int p)
          :: (List.map pool_libs
                ~f:(fun (lib, plib) -> 
                  (lib, plib -. (float p /. libnb))))
      in
      Some (pool, seeding_pM, tot_vol, nm, pool_libs_redistributed)
    | l ->
      perror "Wrong Pool row: %s" (strlist l); None)

(* parse_invoicing --> submission_date, run_type, invoicings *)
let parse_invoicing sanitized =
  let parse_chart piemail stuff =
    let open Option in
    let mandatory msg s = 
      if s = "" then (
        perror "Invoicing for %s: wrong %s: %s (mandatory field)" piemail msg s;
        None) else Some s in
    let optional s = if s = "" then None else Some s in
    let is_n_digits n msg s =
      if String.length s = n && String.for_all s Char.is_digit 
      then Some s else (
        perror "Invoicing for %s: wrong %s: %S should be a \
                       %d-digit string." piemail msg s n; 
        None) in
    let is_n_alphanum n msg s =
      if String.length s = n && String.for_all s Char.is_alphanum 
      then Some s else (
        perror "Invoicing for %s: wrong %s: %S should be a \
                       %d-alphanums string." piemail msg s n; 
        None) in
    match stuff with
    | an :: f :: o :: prog :: proj :: rest ->
      ((mandatory "Account number" an >>= is_n_digits 5 "Account number"),
       (mandatory "Fund" f >>= is_n_digits 2 "Fund"),
       (mandatory "Org" o >>= is_n_digits 5 "Org"),
       (optional prog >>= is_n_alphanum 5 "Program"),
       (mandatory "Project" proj >>= is_n_alphanum 5 "Project"),
       (match rest with [] -> None
       | q :: t when List.for_all t ((=) "") -> optional q
       | q :: t ->
         perror "Wrong row for invoice for %s: %s" piemail
           (strlist stuff); None))
    | _ -> 
      perror "Wrong chartfield for invoice for %s: %s" piemail
        (strlist stuff);
      (None, None, None, None, None, None)
  in
  let section = find_section sanitized "Invoicing" in
  let submission_date, run_type_parsed =
    match sanitized.(section) with
    | "Invoicing" :: "Submission Date:" :: date
      :: "Run Type Requested:" :: run_type_str :: _ ->
      (parse_date date, parse_run_type run_type_str)
    | l ->
      perror "Wrong invoicing row: %s\n" (strlist l);
      (None, None)
  in
  let invoicing_rows =
    get_section sanitized ~start:(section + 1)
      ~first_column_condition:(String.is_prefix ~prefix:"Pool") in
  (submission_date, run_type_parsed,
   List.filter_map invoicing_rows (function
   | piemail :: percent :: chartstuff as row when 
       List.length (String.split ~on:'@' piemail) = 2 ->
     begin match try Some (Float.of_string percent) with _ -> None with
     | Some p -> Some (piemail, p, parse_chart piemail chartstuff)
     | None -> perror "Wrong chartfield row: %s" (strlist row); None
     end
   | l -> perror "Wrong chartfield row: %s" (strlist l); None))


let parse_libraries ~dbh ~(layout: _ Classy.layout) loaded sanitized =
  let check_libname n =
    if String.for_all n ~f:(function
    | '0' .. '9' | 'a' .. 'z' | 'A' .. 'Z' | '-' | '_' -> true
    | _ -> false) then
      ()
    else (
      perror "Wrong library name: %S" n
    ) in
  
  let check_existing_library (libname: string) rest =
    let project =
      match rest with
      | "" :: _ | [] -> None
      | p :: _ -> Some p in
    layout#stock_library#all
    >>| List.filter ~f:(fun l ->
      l#name = libname && l#project = project)
    >>= fun search ->
        (* let search = (\* working around strangety of PGOCaml (c.f.  *)
        (*                 email sent to the mailinglist)  *\) *)
        (*   match project with *)
        (*   | Some _ -> *)
        (*     Layout.Search.record_stock_library_by_name_project *)
        (*       ~dbh libname project *)
        (*   | None -> *)
        (*     Layout.Search.record_stock_library_by_name ~dbh libname *)
        (* in *)
    begin match search with
    | [one] ->
      (* if_verbose "Ok found that library: %d\n" one#g_id; *)
      return (Some one#g_t)
    | [] ->
      perror "Library %s is declared as already defined but was not found%s"
        libname 
        (Option.value_map project ~default:"" ~f:(sprintf " in project %s"));
      return None
    |  l ->
      perror "Library %s has an ambiguous name: %d homonyms%s"
        libname (List.length l)
        (Option.value_map project ~default:"" ~f:(sprintf " in project %s"));
      return None
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

  let filtered_rows =
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
          Some (`known (i, libname, rest))
        | libname :: rest when libname <> ""->
          Some (`new_one (i, libname, rest))
        | _ -> None
      with
      | Invalid_argument "index out of bounds" -> None
      | e -> warning "Saw exception: %s" (Exn.to_string e); None) in

  of_list_sequential filtered_rows (function
  | `known (i, libname, rest) ->
    check_libname libname;
    (* if_verbose "%s should be already known\n" libname; *)
    check_existing_library libname rest
    >>= fun exisiting ->
    let int = int ~msg:(sprintf "Lib %s: " libname) in
    begin match exisiting with
    | Some lib_t ->
      let key_value_list =
        List.map custom_keys 
          (fun c -> (c, column loaded.(section + i + 2) c)) in
      return (
        match rest with
        | [] | [ _ ]
        |  _ :: "" :: []
        |  _ :: "" :: "" :: _ -> 
          `existing (libname, lib_t, None, None, key_value_list)
        |  _ :: conc :: []
        |  _ :: conc :: "" :: _ ->
          `existing (libname, lib_t, int conc, None, key_value_list)
        | _ :: "" :: note :: _ ->
          `existing (libname, lib_t, None, Some note, key_value_list)
        | _ :: conc :: note :: _ ->
          `existing (libname, lib_t, int conc, Some note, key_value_list))
    | None -> return (`wrong libname)
    end
  | `new_one (i, libname, rest) ->
    let row = sanitized.(section + i + 2)  in
    check_libname libname;
    (* if_verbose "%s should be a new one\n" libname; *)
    let open Option in
    let mandatory row col =
      match column row col with
      | Some s -> Some s
      | None -> 
        perror "Lib: %s, mandatory field not provided: %s" libname col; None in
    let mandatory32 r c = 
          mandatory r c >>= int ~msg:(sprintf "Lib: %s (%s)" libname c) in
        let column32 r c = 
          column r c >>= int ~msg:(sprintf "Lib: %s (%s)" libname c) in
        let int = int ~msg:(sprintf "Lib %s: " libname) in
        let project = column row col_Project_Name in
        let conc = column32 row col_Concentration__nM_ in
        let note = column row col_Note in
        let short_desc = column row col_Library_Short_Description in
        let sample_name = column row col_Sample_Name in
        let species = column row col_Species___Source in
        let application = mandatory row col_Application in
        let stranded = mandatory row col_Is_Stranded
                       >>| String.lowercase 
                       >>= function
                       | "true" | "yes" -> Some true
                       | "false" | "no" -> Some false
                       | _ -> None in
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
              | Error "bioo scientific" -> `bioo
              | Error s ->
                perror "Lib: %s cannot recognize barcode provider: %s" libname s;
                `none
              end
            | None -> `none in
        let barcodes =
          match column row col_Barcode_Number with
          | None -> []
          | Some s ->
            String.split ~on:',' s 
            |! List.map ~f:String.strip
            |! List.filter ~f:((<>) "") 
            |! List.filter_map ~f:int
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
            | [ "r1" ; two ] -> int two >>= fun i -> Some (`on_r1 i) 
            | [ "r2" ; two ] -> int two >>= fun i -> Some (`on_r2 i) 
            | [ "i"  ; two ] -> int two >>= fun i -> Some (`on_i i ) 
            | _ ->
              perror "Lib %s: cannot understand custom-barcode-loc: %S" libname s;
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
        Flow.return
          (`new_lib (libname, project, conc, note,
                     short_desc, sample_name, species,
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
                     key_value_list)))
    
let parse ?(dry_run=true) ?(verbose=false) ?(phix=[]) hsc file =
  let if_verbose fmt = 
    ksprintf (if verbose then print_string else Pervasives.ignore) fmt in

  Buffer.clear errbuf;
  printf "========= Loading %S =========\n" file;
  with_database ~configuration:hsc (fun ~dbh ->
    let layout = Classy.make dbh in
    let loaded = Csv.load ~separator:',' file |> Array.of_list in
    (* Csv is not Lwt-compliant... we don't care for now. *)
    let sanitized =
      let sanitize s =
        (String.split_on_chars ~on:[' '; '\t'; '\n'; '\r'] s) |>
            List.filter ~f:((<>) "") |>
                String.concat ~sep:" "
      in
      Array.map loaded ~f:(List.map ~f:sanitize)
    in
    parse_contacts ~verbose ~dbh ~layout sanitized >>= fun contacts ->
    if verbose then (* print contacts *) (
      if_verbose "Contacts:\n";
      List.iter contacts (function
      | email, None, _ -> if_verbose "  %s (TO CREATE)\n" email
      | email, Some o, _ ->
        if_verbose "  %s -- %d\n" email o.Layout.Record_person.id));
    let submission_date, run_type_parsed, invoicing = parse_invoicing sanitized in
    let () = (* invoicing check *)
      let sum = List.fold_left invoicing ~f:(fun x (_, p, _) -> x +. p) ~init:0. in
      if 99. < sum && sum < 101. then
        ()
      else
        perror "Invoicing sums up to %f" sum; in
    if verbose then (* print invoicing *) (
      if_verbose "Invoicing:\n";
      List.iter invoicing (fun (email, p, chartstuff) -> 
        if_verbose "  %s, %f\n" email p
      ););
    
    let pools = parse_pools ~phix sanitized in
    let () = (* check pools *)
      List.iter pools (function
      | Some (pool, _,_,_, l) ->
        let sum = List.fold_left l ~f:(fun x (_, p) -> (x +. p)) ~init:0. in
        if sum < 99. || sum > 101. then
          perror "Pooled percentages for %s do not sum up to 100" pool
      | None -> ()); in
    if verbose then (* print pools *) (
      if_verbose "Pools:\n";
      List.iter pools (function
      | Some (pool, spm, tv, nm, l) -> 
        let f = Option.value_map ~default:"WRONG" ~f:(sprintf "%d") in
        if_verbose "  %s %s %s %s [%s]\n" pool (f spm) (f tv) (f nm)
          (String.concat ~sep:", " 
             (List.map l ~f:(fun (x,y) -> sprintf "%s:%.2f%%" x y)))
      | None -> ()));


    parse_libraries ~dbh ~layout loaded sanitized >>= fun libraries ->
    if verbose then (* print libraries *) (
      if_verbose "Libraries:\n";
      List.iter libraries (function
      | `wrong libname -> if_verbose "  wrong lib: %s\n" libname
      | `existing (s, _, c, n, kv) -> 
        if_verbose "  existing lib: %s%s%s {%s}\n" s
          (Option.value_map c ~default:" (no concentration)" ~f:(sprintf " (%d nM)"))
          (Option.value_map n ~default:" (no note)" ~f:(sprintf " (note: %S)"))
          (String.concat ~sep:", " 
             (List.map kv ~f:(function
             | (k, Some v) -> sprintf "[%S -- %S]" k v
             | (k, None)   -> sprintf "[NO %S]" k)))
      | `new_lib (s, project, conc, note,
                  short_desc, sample_name, species, app, strd, tsc, rsc,
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
            vm bio_wnb ~default:"[NO WNB]" ~f:(sprintf "[wnb: %d]");  
            vm bio_avg ~default:"[NO AVG]" ~f:(sprintf "[avg: %d]");  
            vm bio_min ~default:"[NO MIN]" ~f:(sprintf "[min: %d]");  
            vm bio_max ~default:"[NO MAX]" ~f:(sprintf "[max: %d]"); 
          ]) in
        if_verbose "  new lib: %s\n    %s\n" s
          (String.concat ~sep:"    " ([
            vm project ~default:"[no project]" ~f:(sprintf "[Proj: %s]");
            vm short_desc ~default:"[no desc]" ~f:(sprintf "[Desc: %s]");
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
                               (List.map bcs (Int.to_string)));
            if cbs <> [] then
              sprintf "[Custom: (%s)]" (String.concat ~sep:", " cbs)
            else "";
            if cbp <> [] then
              sprintf "[Position: %s]"
                (String.concat ~sep:", " (List.map cbp ~f:(function
                | `on_r1 i -> sprintf "%d on R1" i
                | `on_r2 i -> sprintf "%d on R2" i
                | `on_i i -> sprintf "%d on Idx" i))) else ""; nl;
            vm p5 ~default:"[NO P5 LENGTH]" ~f:(sprintf "[P5: %d]");
            vm p7 ~default:"[NO P7 LENGTH]" ~f:(sprintf "[P7: %d]"); nl;
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
      ));

    (* More checks *)
    let () =
      let assoc_sample_org =
        List.filter_map libraries ~f:(function
        | `new_lib (s, project, conc, note,
                    short_desc, sample_name, species, app, strd, tsc, rsc,
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
          match List.filter l1 ~f:(fun (x, _) -> x = s) with
          | [ one ] -> ()
          | [] -> failwith "should nt be there"
          | more ->
            perror "Sample %s is defined with too many organisms: %s" s
              (String.concat ~sep:", " 
                 (List.map ~f:snd
                    (List.Assoc.map more (Option.value ~default:"NONE")))));

      let lib_names =
        List.filter_map libraries (function       
        | `wrong libname -> None
        | `existing (libname, lib_t, conc, note, kv) ->  Some libname
        | `new_lib (ln, project, conc, note,
                    short_desc, sample_name, species, app, strd, tsc, rsc,
                    bt, bcs, cbs, cbp,
                    p5, p7,
                    bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                    arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                    protocol_name, protocol_file, preparator, notes, 
                    key_value_list) -> Some ln)
      in
      let check_lib libname =
        if List.find pools 
          ~f:(function
          | Some (pool, spm, tv, nm, input_libs) ->
            (List.find input_libs ~f:(fun (n, _) -> libname = n)) <> None
          | None -> false) = None then
          perror "Lib %S is not used in any pool." libname
      in
      List.iter lib_names check_lib;
      if List.dedup lib_names |! List.length <> List.length lib_names then
        perror "There are duplicates in library names! E.g.: %s"
          (Option.value_exn (List.find_a_dup lib_names));

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
        if p > 0 then
          printf " * [%d] %s\n" p l
        else
          printf " * %s\n" l
      ) in
    let run ~dbh ~fake ~real ~log =
      if dry_run then (
        match List.find !dry_buffer (fun (p, l) -> l = log) with
        | Some (p, _) -> return (fake p)
        | None ->
          incr fake_pointer;
          dry_buffer := (!fake_pointer, log) :: !dry_buffer;
          return (fake !fake_pointer)
      ) else (
        dry_buffer := (0, log) :: !dry_buffer;
        real dbh >>= fun real_thing ->
        Common.add_log ~dbh log >>= fun () ->
        return real_thing
      ) in
    let erroneous_pointer f =
      if dry_run then f (-1)
      else failwith "erroneous_pointer during wet-run!" in

    (* Adding contacts *)
    of_list_sequential contacts (function
      | email, None, contents ->
        let get nth name = 
          match List.nth contents nth with
          | Some (Some s) -> s
          | None | Some None -> 
            perror "Contact %S: %s not provided" email name; 
            (name ^ "-fake")
        in
        let print_name =  List.nth contents 0 |! Option.value ~default:None in
        let given_name = get 2 "Given-name" in
        let middle_name = List.nth contents 3 |! Option.value ~default:None in
        let family_name = get 4 "Family-name" in
        let nickname = List.nth contents 5  |! Option.value ~default:None in
        let login =
          Option.(
            (List.nth contents 6 |! value ~default:None)
            >>= fun l ->
            if String.for_all l Char.is_alphanum
            then Some l else (
              perror "Contact %S: the NetID should be alphanumeric: %S" email l;
              None)) in
        if String.for_all email (function
        | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9'
        | '.' | '-' | '_' | '+' | '@' -> true
        | _ -> false) then () else (
          perror "Email address: %S does not pass-filter …" email;
        );
        run ~dbh
          ~fake:(fun x -> Layout.Record_person.unsafe_cast x)
          ~real:(fun dbh ->
            Access.Person.add_value ~dbh ?password_hash:None
              ~email ~given_name ?middle_name ~family_name ~secondary_emails:[||]
              ?nickname ?login ?print_name ~roles:[| `user |] ?note:None)
          ~log:(sprintf "(add_person %s)" email)
        >>= fun id ->
        return (email, id)
      | email, Some o, _ -> return (email, o))
    >>= fun contacts ->

    (* Adding libraries *)
    let bio_directories = ref [] in
    let arg_directories = ref [] in
    let stock = ref [] in
    of_list_sequential libraries (function
    | `wrong libname -> return ()
    | `existing _ -> return ()    
    | `new_lib (libname, project, conc, note,
                short_desc, sample_name, species, app, strd, tsc, rsc,
                bt, bcs, cbs, cbp,
                p5, p7,
                bio_wnb, bio_avg, bio_min, bio_max, bio_pdf, bio_xad,
                arg_wnb, arg_avg, arg_min, arg_max, arg_img,
                protocol_name , 
                protocol_file , 
                preparator    , 
                notes         , 
                key_value_list) ->
      (* let open Option in *)
      begin match protocol_name with
      | Some name ->
        layout#protocol#all >>| List.filter ~f:(fun p -> p#name = name)
        >>= fun search ->
        begin match search with
        | [] ->
          begin match protocol_file with
          | Some filename ->
            let hr_tag = 
              let buf = Buffer.create 42 in
              let yes s = Buffer.add_char buf s in
              String.iter name (function
              | 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '-' | '_' as c -> yes c 
              | _ -> ());
              Buffer.contents buf  in
            run ~dbh
              ~fake:(fun x -> Layout.File_system.unsafe_cast x)
              ~real:(fun dbh ->
                layout#add_tree_volume ~kind:`protocol_directory
                  ~hr_tag ~files:[ Layout.File_system.Tree.file filename ] ()
                >>| (fun p -> p#pointer))
              ~log:(sprintf "(add_volume protocol_directory %s (files %S))"
                      hr_tag filename)
          | None ->
            perror "Lib: %s, Protocol %S is not in the DB and has no file." 
              libname name;
            return (erroneous_pointer Layout.File_system.unsafe_cast)
          end
          >>= fun prot_vol ->
          run ~dbh
            ~fake:(fun x -> Layout.Record_protocol.unsafe_cast x)
            ~real:(fun dbh ->
              Access.Protocol.add_value ~dbh
                ~name ~doc:prot_vol ?note:None)
            ~log:(sprintf "(add_protocol %S %d)" name prot_vol.Layout.File_system.id)
        | [one] -> return one#g_pointer
        | _ -> failwith "DB Error (record_protocol: too much for one name)"
        end
        >>= fun prot ->
        return (Some prot)
      | None -> return None
      end
      >>= fun prot_opt ->
      of_option species (fun name ->
        layout#organism#all >>| List.filter ~f:(fun o -> o#name = Some name)
        >>= fun search ->
        begin match search with
        | [] ->
          run ~dbh
            ~fake:(fun x -> Layout.Record_organism.unsafe_cast x)
            ~real:(fun dbh ->
              Access.Organism.add_value ~dbh 
                ~name ?informal:None ?note:None)
            ~log:(sprintf "(add_organism %S)" name)
        | [one] -> return one#g_pointer
        | _ -> failwith "DB error (record_organism_by_name)"
        end)
      >>= fun organism ->
      of_option sample_name (fun name ->
        layout#sample#all >>| List.filter ~f:(fun s ->
          if s#name = name then
            match s#project, project with 
            | None, None -> true
            | Some s, None ->
              warning "Sample %s has a project %S in DB but not in subsheet"
                name s;
              false
            | None, Some s ->
              warning "Sample %s has a project %S in this subsheet but not in the DB"
                name s;
              false
            | Some s, Some p -> s = p
          else
            false)
        >>= fun search ->
        begin match search with
        | [] ->
          Layout.Record_sample.(
            run ~dbh ~fake:(fun x -> unsafe_cast x)
              ~real:(fun dbh ->
                Access.Sample.add_value ~dbh 
                  ~name ?organism ?project ?note:None)
              ~log:(sprintf "(add_sample %s%S%s)"
                      (Option.value_map project ~default:"" ~f:(sprintf "%S."))
                      name
                      (Option.value_map organism ~default:""
                         ~f:(fun { Layout.Record_organism.id} ->
                           sprintf " (organism %d)" id))))
        | [one] -> return one#g_pointer
        | more ->
          failwithf "Searching the sample %S got %d results." name
            (List.length more) ()
        end)
      >>= fun sample ->

      let stranded = Option.value ~default:false strd in
      let truseq_control = Option.value ~default:false tsc in

      of_list_sequential cbs (fun sequence -> (* custom barcode seqs *)
        let position_in_r1 =
          List.find_map cbp (function | `on_r1 i -> Some i | _ -> None) in
        let position_in_r2 =
          List.find_map cbp (function | `on_r2 i -> Some i | _ -> None) in
        let position_in_index =
          List.find_map cbp (function | `on_i i -> Some i | _ -> None) in
        run ~dbh ~fake:(fun x -> Layout.Record_custom_barcode.unsafe_cast x)
          ~real:(fun dbh ->
            Access.Custom_barcode.add_value ~dbh
              ?position_in_r1 ?position_in_r2 ?position_in_index ~sequence)
          ~log:(sprintf "(add_custom_barcode %s (r1 %s) (r2 %s) (index %s))"
                  sequence
                  (Option.value_map position_in_r1 ~default:"None" ~f:(sprintf "%d")) 
                  (Option.value_map position_in_r2 ~default:"None" ~f:(sprintf "%d")) 
                  (Option.value_map position_in_index ~default:"None" ~f:(sprintf "%d"))))
      >>| Array.of_list
      >>= fun custom_barcodes ->

      of_option preparator (fun email ->
        begin match List.Assoc.find contacts email with
        | Some id -> return id
        (* |  None ->  *)
        (*   warning "Lib %s: found 'None' contact for %S, \ *)
        (*                 this should be already reported..." libname email; *)
        (*   return (erroneous_pointer Layout.Record_person.unsafe_cast) *)
        | None ->
          layout#person#all >>| List.filter ~f:(fun p -> p#email = email)
          >>= fun search ->
          begin match search with
          | [] ->
            perror "Lib %s: Unknown preparator: %s" libname email;
            return (erroneous_pointer Layout.Record_person.unsafe_cast)
          | [one] -> return one#g_pointer
          | _ -> failwithf "more than one person with email %S" email () 
          end
        end)
      >>= fun preparator ->

      run ~dbh ~fake:(fun x -> Layout.Record_stock_library.unsafe_cast x)
        ~real:(fun dbh ->
          Access.Stock_library.add_value ~dbh 
            ~name:libname ?project ?description:short_desc
            ?sample ?protocol:prot_opt ?application:app ~stranded
            ~truseq_control ?rnaseq_control:rsc
            ~barcode_type:bt ~barcodes:(Array.of_list bcs)
            ~custom_barcodes
            ?p5_adapter_length:p5
            ?p7_adapter_length:p7
            ?preparator
            ?note:None)
        ~log:Option.(
          sprintf "(add_stock_library %s%s %s %s %s)" 
            (value_map project ~default:"" ~f:(sprintf "%s."))
            libname
            (value_map sample ~default:"no_sample"
               ~f:(fun { Layout.Record_sample.id} -> sprintf " (sample %d)" id))
            (value_map prot_opt ~default:"no_protocol"
               ~f:(fun { Layout.Record_protocol.id} -> sprintf " (protocol %d)" id))
            (value_map preparator ~default:"no_preparator"
               ~f:(fun { Layout.Record_person.id} -> sprintf " (preparator %d)" id)))
      >>= fun stock_library ->

      of_option bio_wnb (fun well_number ->
        let dir = List.filter_opt [bio_pdf; bio_xad] in
        let files =
          match List.find !bio_directories (fun (l, i) -> l = dir) with
          | Some (_, i) -> return i
          | None ->
            let hr_tag = sprintf "xad_pdf_%s" libname in
            run ~dbh
              ~fake:(fun x -> Layout.File_system.unsafe_cast x)
              ~real:(fun dbh ->
                Access.Volume.add_tree_volume ~dbh 
                  ~kind:`bioanalyzer_directory
                  ~hr_tag
                  ~files:(List.map dir ~f:(Layout.File_system.Tree.file)))
              ~log:(sprintf 
                      "(add_volume bioanalyzer_directory %s (files (%s)))"
                      hr_tag (String.concat ~sep:" " (List.map dir (sprintf "%S"))))
            >>= fun x ->
            bio_directories := (dir, x) :: !bio_directories;
            return x
        in
        files >>= fun files ->

        let f32o o =
          let open Option in
          bind o (fun x -> return (Float.of_int64 (Int64.of_int x))) in
        run ~dbh ~fake:(fun x -> Layout.Record_bioanalyzer.unsafe_cast x)
          ~real:(fun dbh ->
            Access.Bioanalyzer.add_value ~dbh ~library:stock_library
              ~well_number
              ?mean_fragment_size:(f32o bio_avg) 
              ?min_fragment_size: (f32o bio_min) 
              ?max_fragment_size: (f32o bio_max) 
              ?note:None
              ~files)
          ~log:Option.(sprintf "(add_bioanalyzer %d%s%s)" 
                  (stock_library.Layout.Record_stock_library.id)
                  (value_map bio_wnb ~default:"" ~f:(sprintf " (well_number %d)"))
                  (sprintf " (files %d)" files.Layout.File_system.id)))
      >>= fun bioanalyzer ->

      of_option arg_wnb (fun well_number ->
        let dir = match arg_img with | Some img -> [img] | _ -> [] in
        let files =
          match List.find !arg_directories (fun (l, i) -> l = dir) with
          | Some (_, i) -> return i
          | None ->
            let hr_tag = sprintf "img_%s" libname in
            run ~dbh
              ~fake:(fun x -> Layout.File_system.unsafe_cast x)
              ~real:(fun dbh ->
                Access.Volume.add_tree_volume ~dbh 
                  ~kind:`agarose_gel_directory
                  ~hr_tag
                  ~files:(List.map dir ~f:(Layout.File_system.Tree.file)))
              ~log:(sprintf 
                      "(add_volume agarose_gel_directory %s (files (%s)))"
                      hr_tag (String.concat ~sep:" " (List.map dir (sprintf "%S"))))
            >>= fun x ->
            arg_directories := (dir, x) :: !arg_directories;
            return x
        in
        files >>= fun files ->
        let f32o o =
          let open Option in
          bind o (fun x -> return (Float.of_int64 (Int64.of_int x))) in
        run ~dbh ~fake:(fun x -> Layout.Record_agarose_gel.unsafe_cast x)
          ~real:(fun dbh ->
            Access.Agarose_gel.add_value ~dbh ~library:stock_library
              ~well_number
              ?mean_fragment_size:(f32o arg_avg) 
              ?min_fragment_size: (f32o arg_min) 
              ?max_fragment_size: (f32o arg_max) 
              ?note:None
              ~files)
          ~log:(sprintf "(add_agarose_gel %d%s%s)" 
                  (stock_library.Layout.Record_stock_library.id)
                  (Option.value_map arg_wnb ~default:""
                     ~f:(sprintf " (well_number %d)"))
                  (sprintf " (files %d)" files.Layout.File_system.id)))
      >>= fun agarose_gel ->
      begin match arg_wnb, arg_avg, arg_min, arg_max, arg_img with
      | Some _, Some _, Some _, Some _, Some _ -> ()
      | None, None, None, None, None -> ()
      | _ -> warning "Lib %s: Incomplete Agarose Gel" libname;
      end;
      layout#stock_library#get stock_library
      >>= fun slib_t ->

      stock := (libname, project, slib_t#g_t) :: !stock;
      return ())
    >>= fun _ ->

    of_list_sequential (List.filter_opt pools) (fun (pool, spm, tv, nm, input_libs) ->
      of_list_sequential input_libs (fun (libname, percent) ->
        let find_stock =
          if libname = "PhiX" then (
            layout#stock_library#all
            >>| List.filter ~f:(fun s -> s#name = "PhiX_v3") >>= fun search ->
            begin match search with
            | [one] -> return (one#g_t, None, None, [])
            | _ -> failwithf "Can't find PhiX_v3" ()
            end
          ) else (
            let f = function
              | `wrong libname -> None
              | `existing (ln, lib_t, conc, note, kv) ->
                if libname = ln then Some (lib_t, conc, note, kv) else None
              | `new_lib (ln, project, conc, note,
                          short_desc, sample_name, species, app, strd, tsc, rsc,
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
            in
            match List.find_map libraries ~f with
            | None ->
              perror "Pool: %s, can't find library %S" pool libname;
              failwithf "cannot fake a whole stock library..." ()
          (* return (erroneous_pointer Layout.Record_stock_library.unsafe_cast, None, *)
          (*         Some (sprintf "Completely fake stock library: %s for pool %s" *)
          (*                 libname pool), []) *)
            | Some (s, conc, note, kv) -> return (s, conc, note, kv)
          ) in
        find_stock >>= fun (the_lib, concentration, note, key_values) ->

        let user_db_list =
          List.filter_map key_values (function
          | k, None ->  None
          | key, Some value -> Some (key, value)) in
        of_list_sequential user_db_list (fun (key, value) ->
          run ~dbh  ~fake:(fun x -> Layout.Record_key_value.unsafe_cast x)
            ~real:(fun dbh ->
              Access.Key_value.add_value ~dbh ~key ~value)
            ~log:(sprintf "(add_key_value %S %S)" key value))
        >>| Array.of_list
        >>= fun user_db ->

        let concentration_nM =
          let open Option in
          concentration >>| Int64.of_int >>| Float.of_int64 in
        run ~dbh ~fake:(fun x -> Layout.Record_input_library.unsafe_cast x)
          ~real:(fun dbh ->
            Access.Input_library.add_value ~dbh
              ~library:Layout.Record_stock_library.(pointer the_lib)
              ~submission_date:(Option.value submission_date ~default:(Time.now ()))
              ?volume_uL:None ?concentration_nM ~user_db ?note)
          ~log:Option.(sprintf "(add_input_library %d (submission_date %S)%s \
                                (user_db (%s))%s)" 
                  (the_lib.Layout.Record_stock_library.g_id)
                  (value_map ~f:Time.to_string ~default:"NONE" submission_date)
                  (value_map concentration_nM
                     ~default:"" ~f:(sprintf " (concentration_nM %f)"))
                  (String.concat ~sep:" " 
                     (List.map (Array.to_list user_db) ~f:(fun l ->
                       sprintf "%d" l.Layout.Record_key_value.id)))
                  (value_map note ~default:"" ~f:(sprintf " (note %S)")))
      )
      >>| Array.of_list
      >>= fun input_libraries ->

      let pooled_percentages = List.map input_libs ~f:snd |! Array.of_list in

      let requested_read_length_1, requested_read_length_2 =
        match run_type_parsed with Some (_, l, r) -> (l, r)
        | None -> (if not dry_run then failwith "cannot go further"); (42, None) in

      let contacts = List.map contacts snd |! Array.of_list in
      run ~dbh ~fake:(fun x -> Layout.Record_lane.unsafe_cast x)
        ~real:Option.(fun dbh ->
          Access.Lane.add_value ~dbh ~libraries:input_libraries ~pooled_percentages
            ?seeding_concentration_pM:(spm >>| Int64.of_int >>| Float.of_int64)
            ?total_volume:(tv >>| Int64.of_int >>| Float.of_int64)
            ~requested_read_length_1 ?requested_read_length_2
            ~contacts)
        ~log:(sprintf "(add_lane (libraries (%s)) (percentages %s) \
                          (contacts (%s)))"
                (String.concat ~sep:" " 
                   (List.map (Array.to_list input_libraries) ~f:(fun l ->
                     sprintf "%d" l.Layout.Record_input_library.id)))
                (Array.sexp_of_t Float.sexp_of_t pooled_percentages |! Sexp.to_string) 
                (String.concat ~sep:" " 
                   (List.map (Array.to_list contacts) ~f:(fun l ->
                     sprintf "%d" l.Layout.Record_person.id))))
      >>= fun lane ->
      return (pool, lane)
    )
    >>= fun named_lanes ->

    of_list_sequential invoicing (fun (piemail, p, chartstuff) ->
      begin match List.Assoc.find contacts piemail with
      | Some id -> return id
      | None ->
        layout#person#all >>| List.filter ~f:(fun p -> p#email = piemail)
        >>= fun search ->
        begin match search with
        | [] ->
          perror "Invoice for %s: Unknown PI." piemail;
          return (erroneous_pointer Layout.Record_person.unsafe_cast)
        | [one] -> return one#g_pointer
        | _ -> failwith "DB error searching for P.I."
        end
      end
      >>= fun pi ->

      let account_number, fund, org, program, project, note = chartstuff in
      run ~dbh ~fake:(fun x -> Layout.Record_invoicing.unsafe_cast x)
        ~real:(fun dbh ->
          Access.Invoicing.add_value ~dbh ~pi 
            ~percentage:p
            ~lanes:(Array.map (Array.of_list named_lanes) ~f:snd)
            ?account_number ?fund ?org ?program ?project
            ?note)
        ~log:Option.(sprintf "(add_invoicing (pi %d) (percentage %g) \
                            (lanes (%s))%s%s%s%s%s%s)"
                       pi.Layout.Record_person.id p
                       (String.concat ~sep:" " 
                          (List.map named_lanes ~f:(fun (_, l) ->
                            sprintf "%d" l.Layout.Record_lane.id)))
                       (value_map account_number ~default:""
                          ~f:(sprintf " (account_number %s)"))
                       (value_map fund ~default:"" ~f:(sprintf " (fund %s)"))
                       (value_map org ~default:"" ~f:(sprintf " (org %s)"))
                       (value_map program ~default:"" ~f:(sprintf " (program %s)"))
                       (value_map project ~default:"" ~f:(sprintf " (project %s)"))
                       (value_map note ~default:"" ~f:(sprintf " (note %S)"))))
    >>= fun _ ->

    begin match (Buffer.contents errbuf) with
    | "" -> printf "=== No errors or warnings detected. ===\n"
    | s -> printf "=== ERRORS and WARNINGS: ===\n%s\n" s;
    end;
    print_dry_buffer ();

    printf "=== Lanes ready to use: ===\n";
    List.iter named_lanes ~f:(fun (name, id) ->
      printf "%s --> %d\n" name id.Layout.Record_lane.id
    );
    printf "Charged to %s.\n" (String.concat ~sep:", " 
                                 (List.map invoicing (fun (e, _, _) -> e)));
    return ())
