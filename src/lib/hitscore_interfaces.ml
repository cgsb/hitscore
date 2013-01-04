(** Some shared module types.  *)

open Core.Std

(** Non-I/O Runtime configuration of the library. *)
module type CONFIGURATION = sig

  (** [db_configuration] keeps track of the db-connection parameters. *)
  type db_configuration

  (** Build a data-base configuration. *)
  val db_configuration : host:string ->
    port:int ->
    database:string ->
    username:string -> password:string -> db_configuration

  (** [local_configuration] contains a whole configuration instance. *)
  type local_configuration

  (** Create a [local_configuration], if no [db_configuration] is given,
      the default values will be used (i.e. PGOCaml will try to connect to
      the local database). The default [vol_directory] is ["vol"], the
      default [hiseq_directory] is ["HiSeq"]. *)
  val configure : ?root_path:string ->
    ?root_writers:string list -> ?root_group:string ->
    ?vol_directory:string ->
    ?upload_directory:string ->
    ?db_configuration:db_configuration ->
    ?work_path:string ->
    ?raw_data_path:string ->
    ?hiseq_directory:string ->
    unit -> local_configuration

  (** Get the Data-base configuration *)
  val db: local_configuration -> db_configuration option
    
  val db_host     : local_configuration -> string   option 
  val db_port     : local_configuration -> int      option 
  val db_database : local_configuration -> string   option 
  val db_username : local_configuration -> string   option 
  val db_password : local_configuration -> string   option 

  (** Get the current root path (if set).  *)
  val root_path : local_configuration -> string option
    
  (** Get the root writers' logins. *)
  val root_writers: local_configuration -> string list

  (** Get the root owner group. *)
  val root_group: local_configuration -> string option

  (** Get the name of the volumes directory (i.e. ["vol"]). *)
  val vol_directory: local_configuration -> string
    
  (** Get the current path to the volumes (i.e. kind-of [$ROOT/vol/]). *)
  val vol_path: local_configuration -> string option

  (** Get the name of the uploads directory (i.e. ["upload"] by default). *)
  val upload_directory: local_configuration -> string
    
  (** Get the current path to the uploads (i.e. kind-of [$ROOT/upload/]). *)
  val upload_path: local_configuration -> string option

  (** Make a path to a VFS volume. *)
  val path_of_volume: local_configuration -> string -> string option

  (** Make a path-making function for VFS volumes. *)
  val path_of_volume_fun: local_configuration -> (string -> string) option

  (** Get (if configured) the current work-directory (the one used for
  short-term storage and computations). *)
  val work_path: local_configuration -> string option

  (** Get the path to the raw data (like "/data/cgsb/gencore-raw/"). *)
  val raw_data_path: local_configuration -> string option

  (** Get the relative path to the HiSeq data (like "HiSeq/"). *)
  val hiseq_directory: local_configuration -> string

  (** Get the path to the HiSeq raw data
      (like "/data/cgsb/gencore-raw/HiSeq"). *)
  val hiseq_data_path: local_configuration -> string option
    
  (** Get all the available/configured B2F versions. *)
  val bcl_to_fastq_available_versions: local_configuration -> string list

  (** For a given [version], get the commands to run before. *)
  val bcl_to_fastq_pre_commands: local_configuration -> version:string -> string list

    
  (** Set of available profiles. *)
  type profile_set

  (** Parse a list of S-Expressions representing configuration profiles. *)
  val parse_str: string -> 
    (profile_set, [> `configuration_parsing_error of exn ]) Result.t

  (** Get the list of available profile names.  *)
  val profile_names: profile_set -> string list

  (** Select a profile and build a configuration with it. *)
  val use_profile: profile_set -> string -> 
    (local_configuration, [> `profile_not_found of string]) Result.t


end



(** XML Dom-like representation highly-compatible with XMLM. *)
module  XML = struct
  type name = string * string 
  type attribute = name * string
  type tag = name * attribute list 
  type tree = [ `E of tag * tree list | `D of string ]
end


module Hiseq_raw_information = struct

  (** Parameters of the Hiseq machine found in {e runParameters.xml}.  *)
  type run_parameters = {
    flowcell_name     : string;
    read_length_1     : int;
    read_length_index : int option;
    read_length_2     : int option;
    with_intensities  : bool;
    run_date          : Time.t; }
      
  (** The information returned by the
      [Hiseq_raw.clusters_summary] function.  *)
  type clusters_info = {
    clusters_raw        : float;
    clusters_raw_sd     : float;
    clusters_pf         : float;
    clusters_pf_sd      : float;
    prc_pf_clusters     : float;
    prc_pf_clusters_sd  : float;
  }


end

(** Extract information from Hiseq-raw directories.  *)
module type HISEQ_RAW = sig

  (** Parse {e runParameters.xml}. *)
  val run_parameters: XML.tree -> 
    (Hiseq_raw_information.run_parameters,
     [> `parse_run_parameters of
         [> `wrong_date of string | `wrong_field of string ] ]) Result.t

  (** Parse the [XML.tree] to get cluster densities for each lane (0 to 7).  *)
  val clusters_summary: XML.tree ->
    (Hiseq_raw_information.clusters_info option array,
     [> `parse_clusters_summary of string ]) Result.t

end


module B2F_unaligned_information = struct

  type library_stats = {
    name: string;
    mutable yield: float;
    mutable yield_q30: float;
    mutable cluster_count: float;
    mutable cluster_count_m0: float;
    mutable cluster_count_m1: float;
    mutable quality_score_sum: float;
  }
  type demux_summary = library_stats list array

end

module type B2F_UNALIGNED = sig

  val flowcell_demux_summary: XML.tree ->
    (B2F_unaligned_information.demux_summary,
     [> `parse_flowcell_demux_summary_error of exn]) Result.t

end
