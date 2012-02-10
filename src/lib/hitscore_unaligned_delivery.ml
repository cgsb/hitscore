module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t)
  (Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
     with module Result_IO = Result_IO
     with module Configuration = Configuration
     with module Layout = Layout):

  Hitscore_function_interfaces.UNALIGNED_DELIVERY
  with module Configuration = Configuration
  with module Result_IO = Result_IO
  with module Access_rights = Access_rights
  with module Layout = Layout

  = struct

    module Configuration = Configuration
    module Result_IO = Result_IO
    module Access_rights = Access_rights
    module Layout = Layout

    open Hitscore_std
    open Result_IO

    let volume_relative_paths ~dbh ~configuration vol_pointer =
      Layout.File_system.(
        get_volume ~dbh vol_pointer >>= fun ({volume_entry; _} as volume) ->
        of_result  (volume_trees volume) >>= fun vol_trees ->
        match (Configuration.work_directory configuration) with
        | None -> error (`work_directory_not_configured)
        | Some w ->
          let prefix = w ^ entry_unix_path volume_entry in
          return (List.map (trees_to_unix_paths vol_trees) 
                    ~f:(Filename.concat prefix)))
 
    let debug fmt =
      ksprintf debug fmt

    let run ~(dbh:Layout.db_handle) ~configuration
        ~bcl_to_fastq ~invoice ~destination =
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq >>= function
        | {g_status = `Succeeded; g_result = Some result; _} ->
          return result
        | {g_status} ->
          error (`bcl_to_fastq_not_succeeded (bcl_to_fastq ,g_status)))
      >>= fun unaligned ->
      Layout.Record_bcl_to_fastq_unaligned.(
        get ~dbh unaligned >>= fun {directory} -> return directory)
      >>= volume_relative_paths ~dbh ~configuration 
      >>= (function [one] -> return one | l -> error (`wrong_unaligned_volume l))
      >>= fun unaligned_path ->
      debug "unaligned_path: %S\n" unaligned_path

end
