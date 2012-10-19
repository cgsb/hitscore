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
  layout_cache: (
    ([> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
     | `root_directory_not_configured ] as 'a) Hitscore_layout.Classy.layout_cache) ->
  ('a Hitscore_data_access_types.classy_libraries_information, 'a) Hitscore_std.t

    
val init_classy_libraries_information_loop :
  log:(string ->
       (unit,
        [> `Layout of
            Hitscore_layout.Layout.error_location *
              Hitscore_layout.Layout.error_cause
        | `db_backend_error of
            [> Hitscore_db_backend.Backend.error ]
        | `io_exn of exn
        | `root_directory_not_configured ]
          as 'a) Hitscore_std.t) ->
  loop_withing_time:float ->
  allowed_age:float ->
  maximal_age:float ->
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  unit ->
  ('a Hitscore_data_access_types.classy_libraries_information,
   [> `io_exn of exn]) Hitscore_std.t

val make_classy_persons_information:
  layout_cache: (
    ([> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
     | `root_directory_not_configured ] as 'a) Hitscore_layout.Classy.layout_cache) ->
  ('a Hitscore_data_access_types.classy_persons_information, 'a) Hitscore_std.t
    
