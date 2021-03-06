open Hitscore_std

module Configuration : Hitscore_interfaces.CONFIGURATION = struct

  type db_configuration = {
    db_host     : string;
    db_port     : int;
    db_database : string;
    db_username : string;
    db_password : string
  }

  let db_configuration ~host ~port ~database ~username ~password =
    {db_host = host; db_port = port; db_database = database;
     db_username = username; db_password = password}

  type bcl_to_fastq = {
    b2f_version: string;
    b2f_pre_commands: string list;
  }

  type local_configuration = {
    root_path: string option;
    vol_directory: string;
    root_writers: string list;
    root_group: string option;
    upload_directory: string;
    db_configuration: db_configuration option;
    work_path: string option;
    raw_data_path: string option;
    hiseq_directory: string;
    bcl_to_fastq:  bcl_to_fastq list;
    barcode_data: (string * string * string * int * string) list;
                  (* provider, name, read, position, sequence *)
  }

  let configure ?root_path ?(root_writers=[]) ?root_group
      ?(vol_directory="vol") ?db_configuration ?work_path
      ?raw_data_path ?(hiseq_directory="HiSeq")
      ?(upload_directory="upload")
      ?(bcl_to_fastq=[]) ?(barcode_data=[]) () =
    { root_path; root_writers; root_group; upload_directory;
      vol_directory; db_configuration; work_path;
      raw_data_path; hiseq_directory; bcl_to_fastq; barcode_data}

  let db t = t.db_configuration

  let root_path t = t.root_path

  let vol_directory t = t.vol_directory

  let vol_path t =
    Option.(t.root_path >>| fun r -> sprintf "%s/%s" r t.vol_directory)

  let upload_directory t = t.upload_directory
  let upload_path t =
    Option.(t.root_path >>| fun r -> Filename.concat r t.upload_directory)

  let path_of_volume t volume =
    Option.(vol_path t >>| fun t -> sprintf "%s/%s" t volume)

  let path_of_volume_fun t =
    Option.(vol_path t >>| fun t -> (sprintf "%s/%s" t))

  let root_writers t = t.root_writers
  let root_group t = t.root_group

  let work_path t = t.work_path

  let raw_data_path t = t.raw_data_path
  let hiseq_directory t = t.hiseq_directory
  let hiseq_data_path t =
    Option.(t.raw_data_path >>| fun r -> sprintf "%s/%s" r t.hiseq_directory)

  let bcl_to_fastq_available_versions t =
    List.map t.bcl_to_fastq (fun b -> b.b2f_version)

  let bcl_to_fastq_pre_commands t ~version =
    List.find_map t.bcl_to_fastq (fun b ->
      if b.b2f_version = version then Some b.b2f_pre_commands else None)
    |! Option.value ~default:[]


  let db_host     t = Option.map t.db_configuration (fun dbc -> dbc.db_host    )
  let db_port     t = Option.map t.db_configuration (fun dbc -> dbc.db_port    )
  let db_database t = Option.map t.db_configuration (fun dbc -> dbc.db_database)
  let db_username t = Option.map t.db_configuration (fun dbc -> dbc.db_username)
  let db_password t = Option.map t.db_configuration (fun dbc -> dbc.db_password)

  let barcode_data t = t.barcode_data

  let parse_sexp sexp =
    let fail msg =
      raise (Failure (sprintf "Configuration Syntax Error: %s" msg)) in
    let fail_atom s = fail (sprintf "Unexpected atom: %s" s) in
    let open Sexplib.Sexp in
    let find_field l f =
      List.find_map l (function
      | List [ Atom n; Atom v ] when n = f -> Some v
      | _ -> None) in
    let parse_profile = function
    | Atom o -> fail_atom o
    | List ( Atom "profile" :: Atom name :: l ) ->
      let root_config =
        List.find_map l (function
        | List (Atom "root" :: Atom dir :: l) -> Some (dir, l)
        | _ -> None) in
      let root_path = Option.map root_config fst in
      let root_writers =
        Option.value_map ~default:[] root_config ~f:(fun (_, l) ->
          List.find_map l (function
          | List (Atom "writers" :: l) -> Some l | _ -> None)
          |! Option.map
            ~f:(List.filter_map ~f:(function Atom a -> Some a | _ -> None))
          |! Option.value ~default:[])
      in
      let root_group =
        Option.bind root_config (fun (_, c) ->
          List.find_map c (function
          | List (Atom "group" :: Atom g :: []) -> Some g
          | _ -> None)) in
      let work_path = find_field l "work" in
      let raw_config =
        List.find_map l (function
        | List (Atom "raw" :: Atom dir :: l) -> Some (dir, l)
        | _ -> None) in
      let raw_data_path = Option.map raw_config fst in
      let hiseq_directory =
        Option.bind raw_config (fun (_, c) ->
          List.find_map c (function
          | List (Atom "hiseq" :: Atom d :: []) -> Some d
          | _ -> None)) in
      let bcl_to_fastq =
        List.filter_map l ~f:(function
        | List (Atom "bcl_to_fastq" :: Atom v :: config) ->
          let pre_commands =
            List.filter_map config (function
            | List (Atom "pre_commands" :: c) ->
              Some (List.filter_map c ~f:(function
                  | Atom cmd -> Some cmd
                  | _ -> None))
            | _ -> None) in
          Some {b2f_version = v; b2f_pre_commands = List.concat pre_commands}
        | _ -> None) in
      let db_config =
        List.find_map l (function
        | List (Atom "db" :: l) -> Some l
        | _ -> None) in
      let db_configuration =
        Option.map db_config ~f:(fun l ->
          match find_field l "host", find_field l "port",
            find_field l "database", find_field l "username",
            find_field l "password" with
          | Some host, Some port, Some database, Some username, Some password ->
            db_configuration
              ~host ~port:(Int.of_string port) ~database ~username ~password
          | _ ->
            ksprintf fail "Incomplete DB configuration (profile: %s)" name)
      in
      let barcode_data =
        List.find_map l (function
        | List (Atom "barcodes" :: l) ->
          Some (List.concat_map l (function
            | List (Atom provider :: barcodes) ->
              List.map barcodes (function
                | List [Atom bname; Atom read; Atom position; Atom sequence;] ->
                  begin try
                    (provider, bname, read, Int.of_string position, sequence)
                  with
                    e -> ksprintf fail "Wrong barcode specification: %s, %s (profile: %s): %s"
                           provider bname name (Exn.to_string e)
                  end
                | other ->
                  ksprintf fail "Wrong barcode for %S (profile: %s)" provider name)
            | _ ->
              ksprintf fail "Wrong barcodes (profile: %s)" name))
        | _ -> None)
      in
      (name,
       configure ?work_path ?vol_directory:None ?upload_directory:None
         ?raw_data_path ?hiseq_directory ~bcl_to_fastq ?barcode_data
         ?root_path ~root_writers ?root_group ?db_configuration)
    | _ -> fail "expecting a (profile ...)"
    in
    match sexp with Atom a -> fail a | List l -> List.map l parse_profile

  type profile_set = (string * (unit -> local_configuration)) list

  let parse_str str =
    let sexp =
      try Sexplib.Sexp.of_string (sprintf "(%s)" str)
      with Failure msg ->
        failwith (sprintf "Syntax Error (sexplib): %s" msg)
    in
    try Ok (parse_sexp sexp)
    with
      e -> Error (`configuration_parsing_error e)

  let profile_names pl = List.map pl fst

  let use_profile profiles name =
    match List.Assoc.find profiles name with
    | Some c -> Ok (c ())
    | None -> Error (`profile_not_found name)

  let default_sequencers =  ["CGSB-HS2500-1"; "CGSBAD-HS2000-1" ]

end
