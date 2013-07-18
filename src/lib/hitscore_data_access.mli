open Hitscore_data_access_types

module File_cache : sig

  val get_clusters_info :
    string ->
    (Hitscore_interfaces.Hiseq_raw_information.clusters_info option array,
     [> `parse_clusters_summary of string
     | `read_file_timeout of string * float
     | `read_file_error of string * exn ]) Hitscore_std.t

  val get_demux_summary: string ->
    (Hitscore_interfaces.B2F_unaligned_information.demux_summary,
     [> `parse_flowcell_demux_summary_error of exn
     | `read_file_timeout of string * float
     | `read_file_error of string * exn ]) Hitscore_std.t


  val get_fastx_quality_stats: string ->
    (fastx_quality_stats,
     [> `empty_fastx_quality_stats of string
     | `error_in_fastx_quality_stats_parsing of string * string list list
     | `read_file_timeout of string * float
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
        | `db_backend_error of Hitscore_db_backend.Backend.error
        | `io_exn of exn
        | `root_directory_not_configured ]
          as 'a) Hitscore_std.t) ->
  loop_waiting_time:float ->
  allowed_age:float ->
  maximal_age:float ->
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  unit ->
  ('a Hitscore_data_access_types.classy_libraries_information,
   [> `io_exn of exn]) Hitscore_std.t

val get_fastq_stats:
  'error classy_library ->
  'error submission ->
  'error demultiplexing ->
  Hitscore_interfaces.B2F_unaligned_information.library_stats option

(** For a given aggredated demultiplexing, find the latest successful
    delivery that links the submission's invoice. *)
val choose_delivery_for_user:
  'error demultiplexing ->
  'error submission ->
  ('error delivery * 'error Hitscore_layout.Classy.invoicing_element) option

val user_file_paths:
  unaligned:'error unaligned ->
  submission: 'error submission ->
  'error classy_library ->
  < fastq_r1s : string list;
    fastq_r2s : string list;
    fastx :
      (< kind : string; path : string > list *
       < kind : string; path : string > list) list >

val make_classy_persons_information:
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  layout_cache: (
    ([> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
     | `root_directory_not_configured ] as 'a) Hitscore_layout.Classy.layout_cache) ->
  ('a Hitscore_data_access_types.classy_persons_information, 'a) Hitscore_std.t

val init_classy_persons_information_loop :
  log:(string ->
       (unit,
        [> `Layout of
            Hitscore_layout.Layout.error_location *
              Hitscore_layout.Layout.error_cause
        | `db_backend_error of Hitscore_db_backend.Backend.error
        | `io_exn of exn
        | `root_directory_not_configured ]
          as 'a) Hitscore_std.t) ->
  loop_waiting_time:float ->
  allowed_age:float ->
  maximal_age:float ->
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  unit ->
  ('a Hitscore_data_access_types.classy_persons_information,
   [> `io_exn of exn]) Hitscore_std.t

val init_retrieval_loop:
  log:(string ->
       (unit,
        [> `Layout of
            Hitscore_layout.Layout.error_location *
              Hitscore_layout.Layout.error_cause
        | `db_backend_error of Hitscore_db_backend.Backend.error
        | `io_exn of exn
        | `root_directory_not_configured ]
          as 'a) Hitscore_std.t) ->
  log_prefix:string ->
  loop_waiting_time:float ->
  allowed_age:float ->
  maximal_age:float ->
  configuration:Hitscore_configuration.Configuration.local_configuration ->
  f:(configuration:Hitscore_configuration.Configuration.local_configuration ->
     layout_cache:[> `Layout of
                       Hitscore_layout.Layout.error_location *
                         Hitscore_layout.Layout.error_cause ]
         Hitscore_layout.Classy.layout_cache ->
     ('b, 'a) Hitscore_std.t) ->
  (unit -> ('b, [> `io_exn of exn]) Hitscore_std.t)
(**
   The call
   [init_retrieval_loop ~log ~log_prefix ~loop_waiting_time ~allowed_age
   ~maximal_age ~configuration ~f]:

   - uses [log] to output information (prefixed with [log_prefix])
   - it starts an “update” loop (runs every [loop_waiting_time] seconds)
   - it uses [(allowed_age, maximal_age)] to create a [Classy.layout_cache]
   - in case of error tries to reconnect to the database ([Backend.reconnect])
   - it calls [f ~configuration ~layout_cache] to create the cached value
   - the function returns a function that gets the current cached value
     (uses [Lwt_condition] to wait for the first one to be available)
*)
