(** PBS support. *)
open Hitscore_std

exception Error of string

type mail_option = JobAborted | JobBegun | JobEnded

let mail_option_to_char = function
  | JobAborted -> 'a'
  | JobBegun -> 'b'
  | JobEnded -> 'e'

type script = {
  shell: string;
  mail_options : mail_option list;
  user_list : string list;
  resource_list : string option;
  job_name : string option;
  priority : int option;
  stdout_path : string option;
  stderr_path : string option;
  export_qsub_env : bool;
  rerunable : bool option;
  queue: string option;
  commands : string list;
}

let make_script ?(shell="/bin/bash")
    ?(mail_options=[]) ?(user_list=[]) ?resource_list ?job_name
    ?priority
    ?stdout_path ?stderr_path ?(export_qsub_env=false)
    ?rerunable ?queue
    commands
    =
  {
    shell;
    mail_options = List.dedup mail_options;
    user_list;
    resource_list;
    job_name;
    priority;
    stdout_path;
    stderr_path;
    export_qsub_env;
    rerunable;
    queue;
    commands
  }

let script_to_string x =
  let e opt = sprintf "#PBS %s\n" opt in
  let s opt x = sprintf "#PBS %s %s\n" opt x in
  let i opt x = sprintf "#PBS %s %d\n" opt x in
  let header = String.concat ~sep:"" [
    sprintf "#!%s\n" x.shell;
    (match x.mail_options with
      | [] -> ""
      | x ->
          let x =
            List.map ~f:(fun x -> mail_option_to_char x |> String.of_char) x in
          s "-m" (String.concat ~sep:"" x)
    );
    (match x.user_list with
      | [] -> ""
      | x -> s "-M" (String.concat ~sep:"," x)
    );
    if x.export_qsub_env then e "-V" else "";
    (match x.rerunable with
      | None -> ""
      | Some x -> s "-r" (if x then "y" else "n")
    );
    (match x.resource_list with None -> "" | Some x -> s "-l" x);
    (match x.priority with None -> "" | Some x -> i "-p" x);
    (match x.stdout_path with None -> "" | Some x -> s "-o" x);
    (match x.stderr_path with None -> "" | Some x -> s "-e" x);
    (match x.job_name with None -> "" | Some x -> s "-N" x);
    (match x.queue with None -> "" | Some q -> s "-q" q);
  ]
  in
  header ^ "\n" ^ (String.concat ~sep:"\n" x.commands) ^ "\n"

let script_to_file script ?perm file : unit =
  Out_channel.with_file ?perm file ~f:(fun cout ->
    fprintf cout "%s\n" (script_to_string script))


let make_and_run ?(resource_list="nodes=1:ppn=8,mem=14gb") ~job_name ?(export_qsub_env=true) outdir commands =
  let pbs_stdout_file = Filename.concat outdir "stdout.txt" in
  let pbs_stderr_file = Filename.concat outdir "stderr.txt" in
  let pbs_script_file = Filename.concat outdir "script.pbs" in
  let qsub_out_file = Filename.concat outdir "qsub_out.txt" in

  let script = make_script
    (* ~mail_options:[JobAborted; JobBegun; JobEnded] *)
    (* ~user_list:["ashish.agarwal@nyu.edu"] *)
    ~resource_list
    (* ~priority:(-1024) *)
    ~job_name
    ~stdout_path:pbs_stdout_file
    ~stderr_path:pbs_stderr_file
    ~export_qsub_env
    ~rerunable:false
    commands
  in

  Unix.mkdir outdir ~perm:0o755;
  script_to_file script ~perm:0o644 pbs_script_file;
  let cmd = sprintf "qsub %s > %s 2>&1" pbs_script_file qsub_out_file in
  print_endline cmd;
  match Sys.command cmd with
    | 0 -> ()
    | x -> eprintf "qsub returned exit code %d\n" x


type job_stats_raw = string * (string * string) list
(** The output of [parse_pbs]: ["Job official ID", (key, value) list]. *)

(** Parse the output of [qstat -f1 <job-id>] (for now this does not
    handle output for multiple jobs). *)
let parse_qstat (s: string) : (job_stats_raw, _) Core.Std.Result.t =
  let open Core.Std in
  let open Result in
  let option_or_fail e o =
    match o with Some s -> return s | None -> fail e in
  let lines =
    String.split ~on:'\n' s |! List.map ~f:String.strip
    |! List.filter ~f:((<>) "") in
  List.hd lines |! option_or_fail (`no_header s)
  >>= fun header ->
  String.lsplit2 header ~on:':' |! option_or_fail (`wrong_header_format header)
  >>| snd
  >>| String.strip
  >>= fun official_job_id ->
  List.tl lines |! option_or_fail (`no_header s)
  >>= fun actual_info ->
  let oks, errors =
    List.map actual_info (fun line ->
      String.lsplit2 line ~on:'=' |! option_or_fail (`wrong_line_format line)
      >>= fun (key, value) ->
      return (String.strip key, String.strip value))
    |! List.partition_map  ~f:(function | Ok o -> `Fst o | Error e -> `Snd e) in
  begin if errors <> []
    then fail (`errors errors)
    else return (official_job_id, oks)
  end

(** Get the status of the job (this follows
{{:http://linux.die.net/man/1/qstat-torque}the qstat-torque manpage}).*)
let get_status ((_, assoc): job_stats_raw) =
  let open Core.Std.Result in
  match Core.Std.List.Assoc.find assoc "job_state" with
  | Some "R" -> return `Running
  | Some "Q" -> return `Queued
  | Some "C" -> return `Completed
  | Some "E" -> return `Exiting
  | Some "H" -> return `Held
  | Some "T" -> return `Moved
  | Some "W" -> return `Waiting
  | Some "S" -> return `Suspended
  | Some s -> fail (`unknown_status s)
  | None -> fail `job_state_not_found

