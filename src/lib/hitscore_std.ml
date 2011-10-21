


include BatPervasives
include BatPrintf

module List = struct 
  include List
  include ListLabels
  include BatList
  include BatList.Labels

  let assoc_exn = assoc
  let hd_exn = hd
  let tl_exn = tl

  include BatList.Exceptionless
  include BatList.Labels.LExceptionless

end

module String = struct
  include StringLabels
  include BatString
  include BatString.Exceptionless
  let split_exn a b = BatString.split a b
end

module Array = struct
  include Array 
  include BatArray 
  include BatArray.Labels
end

module Enum = struct
  include BatEnum
  include BatEnum.Labels
  include BatEnum.Labels.LExceptionless
end

module Util = struct

  let now () =
    CalendarLib.Calendar.now (), CalendarLib.Time_Zone.current()

  let file_last_modified_time file =
    let open Unix in
    let open CalendarLib in
    ((stat file).st_mtime |> localtime |> Calendar.from_unixtm),
    Time_Zone.Local
end

module Map = BatMap
module IntMap = BatMap.IntMap
module Set = BatSet
module StringSet = BatSet.StringSet
