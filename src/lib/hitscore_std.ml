include Core.Std

module Util = struct

  let now () =
    CalendarLib.Calendar.now (), CalendarLib.Time_Zone.current()

  let file_last_modified_time file =
    let open Unix in
    let open CalendarLib in
    ((stat file).st_mtime |! localtime |! Calendar.from_unixtm),
    Time_Zone.Local
end

module IntMap = Map.Make(Int)
module StringSet = Set.Make(String)

type dbh = (string, bool) Hashtbl.t PGOCaml.t
