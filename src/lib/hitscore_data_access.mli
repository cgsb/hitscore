open Hitscore_data_access_types
  
module File_cache : sig

  val get_clusters_info :
    string ->
    (Hitscore_interfaces.Hiseq_raw_information.clusters_info option array,
     [> `parse_clusters_summary of string
     | `read_file_error of string * exn ]) Hitscore_std.t

  val get_demux_summary: string ->
    (Hitscore_interfaces.B2F_unaligned_information.demux_summary,
     [> `parse_flowcell_demux_summary_error of exn
     | `read_file_error of string * exn ]) Hitscore_std.t


  val get_fastx_quality_stats: string ->
    (fastx_quality_stats,
     [> `empty_fastx_quality_stats of string
     | `error_in_fastx_quality_stats_parsing of string * string list list
     | `read_file_error of string * exn ]) Hitscore_std.t

end

  

val make_classy_libraries_information:
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  dbh:Hitscore_db_backend.Backend.db_handle ->
  (([> `Layout of
      Hitscore_layout.Layout.error_location *
        Hitscore_layout.Layout.error_cause
    | `root_directory_not_configured ] as 'a)
      Hitscore_data_access_types.classy_libraries_information, 'a) Hitscore_std.t

val filter_classy_libraries_information :
  qualified_names:string list ->
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  people_filter:(Hitscore_layout.Layout.Record_person.pointer list ->
                 (bool,
                  [> `Layout of
                      Hitscore_layout.Layout.error_location *
                        Hitscore_layout.Layout.error_cause ] as 'a) Hitscore_std.t) ->
  'a classy_libraries_information ->
  ('a Hitscore_data_access_types.classy_library list, 'a) Hitscore_std.t

