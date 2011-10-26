(* ocamlfind ocamlc -thread -package core,sexplib,sexplib.syntax -syntax camlp4o -linkpkg dsl.ml -o dsl *)
(* ocamlfind ocamlc -thread -package core -linkpkg dsl.ml -o dsl *)

open Printf

let (|>) x f = f x
let (@) = Core.Core_list.append

open Core.Result.Export
module Result = struct
  include Core.Result
end

module List = struct
  include Core.Core_list

  (* When f : 'a -> (unit, 'b) result and l : 'a list,
     results ~f l returns Ok () if every element in the list gets Ok (),
     or Error b for the first which gets Error b *)
  let results ~f l =
    let o =
      find_map ~f:(fun x ->
        match f x with Ok () -> None | Error b -> Some (Error b)) l in
    match o with 
    | None -> Ok ()
    | Some b -> b

end

module String = Core.Core_string

module Fake_sexpable (M : sig type t val empty : t end) = struct
  type sexpable = M.t
  let t_of_sexp: Sexplib.Sexp.t -> sexpable = fun _ -> M.empty
  let sexp_of_t: sexpable -> Sexplib.Sexp.t = fun _ -> Sexplib.Sexp.Atom "THE_UGLY_Fake_sexpable_HACK"
end

module Path = struct
  type path = String.t List.t
  type t = path

  let str = String.concat ~sep:"/"

  let is_top_of left right =
    let l = ref right in
    List.for_all ~f:(fun name ->
      match !l with
      | h :: t -> 
        l := t;
        h = name
      | [] -> false) left

  let dir_path path =
    List.take path (List.length path - 1)

  let compare a b = String.compare (str a) (str b)

  let concat a = List.concat a

  let equal = (=)
  let hash = Hashtbl.hash
  let compare = compare

  include Fake_sexpable(struct type t = path let empty = [] end)
end

module Path_HT = struct
  include Core.Hashable.Make (Path)
  include Table
  let add = replace

end

module Path_set = struct
  type t = Path.t list
  let empty = []
  let add t e = e :: t
  let to_list t = List.rev t
end


module Option = Core.Option

module String_tree = struct

  type t = 
    | Elt of string
    | Cat of t list
    | Sep_cat of string * t list

  let elt e = Elt e
  let concat ?sep l = 
    match sep with 
    | None -> Cat l
    | Some s -> Sep_cat (s, l)
  let nl = Elt ""

  let rec iter f = function
    | Elt e -> f e
    | Cat le -> List.iter ~f:(iter f) le
    | Sep_cat (sep, []) -> ()
    | Sep_cat (sep, h :: t) -> 
      iter f h;
      List.iter t ~f:(fun ls -> f sep; iter f ls)

end

module System = struct
  (** create a directory but doesn't raise an exception if the
      directory * already exist *)
  let mkdir_safe dir perm =
    try Unix.mkdir dir perm with Unix.Unix_error (Unix.EEXIST, _, _) -> ()

(** create a directory, and create parent if doesn't exist
    i.e. mkdir -p *)
  let mkdir_p ?(perm=0o700) dir =
    let rec p_mkdir dir =
      let p_name = Filename.dirname dir in
      if p_name <> "/" && p_name <> "." then 
        p_mkdir p_name;
      mkdir_safe dir perm in
    p_mkdir dir 

  let cmd s =
    match Unix.system s with
    | Unix.WEXITED 0 -> Ok ()
    | err -> Error err
      
end

module DSL = struct

  module Definition = struct

    type dsl_type = 
      | T_int
      | T_string
      | T_fastq
      | T_file 



    type fastq = {
      fastq_comment : string;
      fastq_record : string;
      fastq_quality: string;
    }

    type atom =
      | Int of int
      | String of string
      | Fastq of fastq list
      | File of string

    type expression =
      | Constant of atom
      | Variable of string
      | External of Path.t * dsl_type
      | Load_fastq of expression
      | Bowtie of expression * expression (* fastq x some_other -> ??? *)

    type global = string * expression

    type program = global list

  end

  open Definition


  let string_of_type = function
    | T_int -> "int"
    | T_string -> "string"
    | T_fastq -> "fastq"
    | T_file -> "file"

  let string_of_expression e =
    let fastq = fun { fastq_comment; fastq_record; fastq_quality } ->
      sprintf "(%s %s %s)" fastq_comment fastq_record fastq_quality in
    let basic = function
      | Int i -> sprintf "%d" i
      | String s -> sprintf "%S" s
      | Fastq f -> 
        sprintf "(fastq %s)" (String.concat ~sep:", " (List.map ~f:fastq f))
      | File f -> sprintf "(file %s)" f in
    let rec expr = function
      | Constant c -> sprintf "(cst %s)" (basic c)
      | Variable v -> sprintf "(var %s)" v
      | External (p, t) ->
        sprintf "(ext %s : %s)" (Path.str p) (string_of_type t)
      | Load_fastq e -> sprintf "(load_fastq %s)" (expr e)
      | Bowtie (e1, e2) -> sprintf "(bowtie %s %s)" (expr e1) (expr e2)
    in
    (expr e)

  let print_program ?(indent=0) =
    let strindent = String.make indent ' ' in
    List.iter ~f:(fun (g, e) ->
      printf "%s[%S:\n%s  %s\n%s]\n" 
        strindent g strindent (string_of_expression e) strindent)




  module Construct = struct
      
    let int i = Constant (Int i)
    let str s = Constant (String s)
    let var s = Variable s
    let file f = Constant (File f)
    let load_fastq f = Load_fastq f 
    let bowtie e i = Bowtie (e, int i)
    let ext_int p = External (p, T_int)
    let ext_str p = External (p, T_string)
    let ext_fastq p = External (p, T_fastq)
    let ext_file p = External (p, T_file)
  end

  module Verify = struct

    let identifier var =
      let module N = struct exception O of string end in
      try
        String.iter var ~f:(function
          | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' -> ()
          | c -> 
            raise (N.O (sprintf
                           "Char %C not allowed in variable name: %S" c var))
        );
        if String.length var > 2 && String.sub var 0 2 = "__" then
          raise (N.O (sprintf "Variable %S can't start with '__'" var));
        Ok ()
      with
        N.O msg -> Error msg
                 


    let variable_type env var =
      match identifier var with
      | Ok () ->
        begin match List.find ~f:(fun (v, _) -> v = var) env with
        | Some (_, t) -> t
        | None -> Error (sprintf "Variable %S not found" var)
        end
      | Error b -> Error b

    let type_check_expression 
        ?(check_externals:(Path.t -> (dsl_type, string) Result.t) option)
        ?(check_files: string -> (unit, string) Result.t=fun _ -> Ok ())
        environment expresssion =
      let rec descent env expr =
        match expr with
        | Constant a -> 
          begin match a with
          | Int _ -> Ok T_int
          | String _ -> Ok T_string
          | Fastq _ -> Ok T_fastq
          | File f -> Result.bind (check_files f) (fun () -> Ok T_file)
          end
        | Variable v -> variable_type env v
        | External (p, t) -> 
          Result.bind (List.results ~f:identifier p)
            (fun () -> 
              match check_externals with
              | Some ce ->
                begin match ce p with 
                | Ok et -> 
                  if et = t then
                    Ok t 
                  else
                    Error
                      (sprintf "External %s has conflicting types: %s Vs %s"
                         (Path.str p) (string_of_type et) (string_of_type t))
                | Error b -> Error b
                end
              | None -> Ok t)
        | Load_fastq e -> 
          begin match descent env e with
          | Ok T_file -> Ok T_fastq
          | Ok other ->
            Error (sprintf "Load_fastq expects a file, %S has a wrong type: %S"
                   (string_of_expression e) (string_of_type other))
          | Error s -> Error s
          end
        | Bowtie (e1, e2) ->
          begin match descent env e1, descent env e2 with
          | Ok T_fastq, Ok T_int -> Ok T_fastq
          | _, _ ->
            Error "Bowtie expects a `fastq and an `int … error messages \
                  will be better in the future"
          end
      in
      descent environment expresssion
      
    let type_check_program ?check_externals ?check_files p =
      let env = ref [] in
      List.iter ~f:(fun (name, expr) ->
        let tc = 
          Result.bind (identifier name)
            (fun () -> type_check_expression
              ?check_externals ?check_files !env expr)
        in
        env := (name, tc) :: !env
      ) p;
      List.rev !env

  end



  module Runtime = struct

    (* Representation of the server run-time in the meta-language *)
    module Simulation_backend = struct

      type error = [ 
      | `fatal_error of string (* The kind that should not happen *)
      | `wrong_request of string (* The “client” sent an impossible request *)
      | `runtime_error of string (* Error due to external conditions *)
      ]
      let string_of_error = function
        | `fatal_error   s -> sprintf "FATAL ERROR: \"%s\"\n\
                                       ===>  you should complain to the devs" s
        | `wrong_request s -> sprintf "Wrong Request: %s" s 
        | `runtime_error s -> sprintf "Runtime Error: %s" s 
      exception Error_exn of error

      type shell_script = {
        sh_toplevel: String_tree.t;
        sh_value: String_tree.t;
        sh_dependencies: Path.t list;
      }

      type meta_value = 
        | RT_int of int
        | RT_string of string
        | RT_file of string
        | RT_fastq of fastq
        | RT_expression of expression * dsl_type
        | RT_compiled of shell_script * dsl_type

      type runtime_value = {
        (* kind of path / unique id / Ocsigen's convention *)
        val_id: Path.t;
        (* again a stack, representing history *)
        mutable val_history: meta_value list;
      }

      let rt_value id v = {val_id = id; val_history = [ v ]}
      let update_value v nv =
        v.val_history <- nv :: v.val_history
      let current_value v =
        match List.hd v.val_history with
        | None ->
          raise (Error_exn
                   (`fatal_error
                       (sprintf "The value %S has no history!"
                          (Path.str v.val_id))))
        | Some e -> e

      let string_of_runtime_value v =
        let s = 
          match current_value v with
          | RT_int            v    -> sprintf "INT %d" v 
          | RT_string         v    -> sprintf "STRING %S" v
          | RT_file           v    -> sprintf "FILE %s" v 
          | RT_fastq          v    -> sprintf "FASTQ %s" v.fastq_comment 
          | RT_expression   (v, t) -> sprintf "EXPR(%s) %s"
            (string_of_type t) (string_of_expression v)
          | RT_compiled (sh, t) -> sprintf "COMPILED(%s)" (string_of_type t)
        in
        sprintf "VAL:%S = [ %s; ... %d updates ... ]"
          (Path.str v.val_id)  s (List.length v.val_history - 1)

      type runtime = {
        unique_id: string -> string;
        mutable programs: (string * (string * expression * dsl_type) list) list;
        mutable values:  runtime_value Path_HT.t;
      }
      let create () =
        let ids = ref 0 in
        {unique_id = (fun s -> incr ids; sprintf "%s%d" s !ids);
         programs = [];
         values = Path_HT.create ()}
      let add_value rt v =
        Path_HT.add rt.values v.val_id v
      let get_value rt id =
        Path_HT.find rt.values id
      exception Found of (Path.t * runtime_value)
      let find_value ~f rt =
        try
          Path_HT.iter rt.values ~f:(fun ~key:path ~data:v ->
            if f path v then
              raise (Found (path, v))
          );
          None
        with
          Found (p, v) -> Some (p, v)
      let type_of_value rt path =
        match get_value rt path with
        | Some s ->
          begin match current_value s with
          | RT_int            v    -> Ok T_int
          | RT_string         v    -> Ok T_string
          | RT_file           v    -> Ok T_file  
          | RT_fastq          v    -> Ok T_fastq
          | RT_expression   (v, t) -> Ok t
          | RT_compiled   (v, t) -> Ok t
          end
        | None -> Error (sprintf "Value %s not found" (Path.str path))
        
      let print_runtime ?(indent=0) rt =
        let strindent = String.make indent ' ' in
        Path_HT.iter rt.values (fun ~key ~data:v ->
          printf "%s%s\n" strindent (string_of_runtime_value v)
        ) 

      let load_program rt prog_id program =
        let type_checked = 
          Verify.type_check_program  program
            ~check_externals:(type_of_value rt)
            ~check_files:(fun path ->
              if Sys.file_exists path then Ok () else
                Error (sprintf "File %S does not exist" path))
        in
        let validate () =
          if program = [] then failwith "Won't load an empty program";
          List.map2_exn ~f:(fun (name, tres) (name, code) ->
            match tres with
            | Ok t -> 
              let path = [prog_id; name] in
              begin match get_value rt path with
              | Some s ->
                failwith (sprintf "Program/Module %S already exists"
                            (Path.str path))
              | None -> (name, code, t)
              end
            | Error reason ->
              failwith 
                (sprintf "Can't load programs that do not type-check: %s: %s"
                   name reason)
          ) type_checked program
        in
        try 
          let validated = validate () in
          rt.programs <- (prog_id, validated) :: rt.programs;
          List.iter ~f:(fun (name, expr, t) ->
            let path = [prog_id; name] in
            add_value rt (rt_value path (RT_expression (expr, t)))
          ) validated;
          Ok ()
        with
          Failure s -> 
            Error (`wrong_request s)

      module Transform = struct

        type optimization = [ `to_eleven ]

        let factorize_files ?(filter=[]) rt =
          let new_file =
            (fun filename ->
              let f path v =
                match current_value v with
                | RT_file f -> f = filename
                | _ -> false in
              match find_value ~f rt with
              | Some (p, f) -> External (p, T_file)
              | None ->
                let name = rt.unique_id "__file_" in
                let path = [ "__files"; name ] in
                add_value rt (rt_value path (RT_file filename));
                External (path, T_file)) in
          let rec transform_expressions = function
            | Constant (File f) -> new_file f
            | Constant _ as e -> e
            | Variable _ as e -> e
            | External _ as e -> e
            | Load_fastq  e -> Load_fastq (transform_expressions e)
            | Bowtie (e1, e2) -> 
              Bowtie (transform_expressions e1, transform_expressions e2) in
          Path_HT.iter rt.values (fun ~key:path ~data:v ->
            if Path.is_top_of filter path then
              begin match current_value v with
              | RT_expression (e, t) -> 
                let new_expr = transform_expressions e in
                if e <> new_expr then
                  update_value v (RT_expression (new_expr, t))
              | _ -> ()
              end
          )

        let eval_constants  ?(filter=[]) rt =
          Path_HT.iter rt.values (fun ~key:path ~data:v ->
            if Path.is_top_of filter path then
              begin match current_value v with
              | RT_expression (Constant (File f), t) -> 
                update_value v (RT_file f)
              | RT_expression (Constant (Int i), t) -> 
                update_value v (RT_int i)
              | RT_expression (Constant (String s), t) -> 
                update_value v (RT_string s)
              | _ -> ()
              end
          ) 
       
        let optimize ?filter (how: optimization) rt =
          match how with
          | `to_eleven ->
            eval_constants ?filter rt; 
            factorize_files ?filter rt

        let compile rt prog =
          let open String_tree in
          let compile_atom = function
            | Int      v -> string_of_int v
            | String   v -> v
            | Fastq    v -> "Is_there_really_a_fastq_atom_or_just_a_type"
            | File     v -> v
          in
          let echo s = elt (sprintf "echo \"%s\" >> $MONITORING\n" s) in
          let toplevel = 
            ref (elt (sprintf "# Program %s compiled top-level\n" 
                        (Path.str prog))) in
          let to_top t = toplevel := concat (!toplevel :: t) in
          let dependencies = ref Path_set.empty in
          let depends_on s = dependencies := Path_set.add !dependencies s in
          let rec compile_expression = function
            | Constant a -> elt (compile_atom a)
            | Variable v -> 
              let full_name = Path.(concat [(dir_path prog); [v]]) in
              depends_on full_name;
              elt (sprintf "`$get_result %s`" (Path.str full_name))
            | External (e, t) ->
              depends_on e;
              elt (sprintf "`$get_result %s`" (Path.str e))
            | Load_fastq  e -> 
              let file = compile_expression e in
              let file_var = rt.unique_id "FASTQ_LOADED_FILE_" in
              to_top [
                echo "Checking file";
                elt "if [ -f "; file; elt " ] ; then\n";
                echo "Seems OK";
                elt "else\n";
                echo "Problem!";
                elt "exit 2\nfi\nexport ";
                elt file_var;
                elt "="; file; elt "\n\n"
              ];
              elt (sprintf "$%s" file_var)
            | Bowtie (e1, e2) -> 
              let fastq = compile_expression e1 in
              let int = compile_expression e2 in
              let output_file = rt.unique_id "bowtie_output_" in
              let result_var = rt.unique_id "BOWTIE_RESULT_" in
              to_top [
                elt "echo 'Call Bowtie' >> $MONITORING\n";
                elt "$BOWTIE "; elt output_file; elt " -fastq-file "; fastq;
                elt " -param "; int; elt "\n";
                elt "export "; elt result_var; elt "=$?\n";
                elt "if [ $"; elt result_var; elt " -eq 0 ]; then\n";
                echo (sprintf 
                        "Bowtie was successful (ret: $%s ; file: %s)"
                        result_var output_file);
                elt "else\n";
                echo (sprintf "Bowtie gave an error (ret: $%s)" result_var);
                elt (sprintf "  rm -f %s\nfi\n" output_file);
              ];
              concat [ elt "$get_result "; elt output_file;]
          in
          match get_value rt prog with
          | None -> raise (Error_exn (`wrong_request
                                     (sprintf "Program not found: %S"
                                        (Path.str prog))))
          | Some value ->
            begin match current_value value with
            | RT_expression (e, t) ->
              let compiled = compile_expression e in
              let result = {
                sh_toplevel = !toplevel;
                sh_value = compiled;
                sh_dependencies = Path_set.to_list !dependencies;
              } in
              update_value value (RT_compiled (result, t))
            | _ -> ()
            end

      end

      let compile_runtime ?(optimize=`to_eleven) ?filter rt =
        Transform.optimize optimize ?filter rt;
        Path_HT.iter rt.values  ~f:(fun ~key:path ~data ->
          match filter with
          | None -> Transform.compile rt path
          | Some fil ->
            if Path.is_top_of fil path then Transform.compile rt path);
        ()

      let assemble_program rt prog exec_path =
        let common_header =
          sprintf "# Common Header\n
export EXEC_PATH=%s
export MONITORING=$EXEC_PATH/monitoring
echo 'Starting program' > $MONITORING
date -R >> $MONITORING
echo '' >> $MONITORING

fun_get_result () {
  echo \"Getting file $1\" >> $MONITORING
  echo $EXEC_PATH/$1
}
export get_result=fun_get_result
fun_store_result () {
  echo \"Storing $1 into $2\" >> $MONITORING
  cp $1 $EXEC_PATH/$2
}
fun_bowtie () {
  echo \"fun_bowtie called with '$*'\" >> $MONITORING
  # sleep 2
  echo $* >> $EXEC_PATH/$1
  echo \"$3 :\" >> $EXEC_PATH/$1
  cat $3 >> $EXEC_PATH/$1
}
export BOWTIE=fun_bowtie

# start 
sleep 2
echo 'After initial sleep' >> $MONITORING

" exec_path 
        in
        match get_value rt prog with
        | None -> raise (Error_exn (`wrong_request
                                   (sprintf "Program not found: %S"
                                      (Path.str prog))))
        | Some value ->
          begin match current_value value with
          | RT_compiled (com, t) ->
            let open String_tree in
            let rec get_dep p = 
              match get_value rt p with
              | None ->
                raise (Error_exn (`fatal_error
                                 (sprintf "Program %S has a dependency not found: %S"
                                    (Path.str prog) (Path.str p))))
              | Some s ->
                begin match current_value s with
                | RT_compiled (c, _) ->
                  concat [
                    concat (List.map c.sh_dependencies ~f:get_dep);
                    c.sh_toplevel;
                    elt "fun_store_result `"; c.sh_value; elt "` ";
                    elt (Path.str p); elt "\n";
                  ]
                | RT_string _ | RT_int _ | RT_file _ | RT_fastq _ ->
                  elt (sprintf "# %S is constant\n" (Path.str p))
                | RT_expression _ ->
                  raise (Error_exn
                           (`wrong_request
                               (sprintf "Program %S has a dependency not compiled: %S"
                                  (Path.str prog) (Path.str p))))
                end
            in
            (* printf "@@@ %S has %d dependencies: %s\n" *)
            (*   (Path.str prog) (List.length com.sh_dependencies)  *)
            (*   (String.concat ", " (List.map Path.str com.sh_dependencies)); *)
            let toplevel =
              concat [
                elt common_header;
                concat (List.map com.sh_dependencies ~f:get_dep);
                com.sh_toplevel;
                elt "fun_store_result `"; com.sh_value; elt "` ";
                elt (Path.str prog); elt "\n";
              ] in
            (com.sh_value, toplevel)
          | _ ->
            raise (Error_exn (`wrong_request
                             (sprintf "Program not compiled: %S"
                                (Path.str prog))))
          end

      let run_program rt prog =
        (* Prepare file system *)
        let exec_dir = 
          let path = rt.unique_id "/tmp/sequme_runtime_" in
          System.mkdir_p path;
          path in
        Path_HT.iter rt.values ~f:(fun ~key ~data ->
          match current_value data with
          | RT_file f -> 
            let path =
              (sprintf "%s/%s" exec_dir Path.(key |> dir_path |> str)) in
            System.mkdir_p path;
            ignore (System.cmd (sprintf "cp %s %s/%s" f exec_dir (Path.str key)))
          | _ -> ()
        );
        let get_value, script = assemble_program rt prog exec_dir in
        let script_file =
          let file = rt.unique_id "sequme_script_" in
          sprintf "%s/%s" exec_dir file in
        let out = open_out script_file in
        String_tree.iter (output_string out) script;
        close_out out;
        match System.cmd (sprintf "nohup sh %s &" script_file) with
        | Ok () ->
          printf "%s is running\n" script_file
        | Error b ->
          printf "%s did not start\n" script_file


    end

  end


  (* Client or Server side *)
  let check_program prog =
    (* syntax / type checking / (optional) database check *)
    ()

  (* Client to Server transfer *)
  let load_program prog =
    (* if prog not trusted i.e. from client: re-check  *)
    (* create id, add program to database *)
    "prog unique id"

  (* On Server *)
  let compile_program prog_id other_parameters =
    (* look for internal and external redundancies *)
    (* optimize *)
    (* prepare the PBS script *)
    ()

  (* On Server *)
  let run_program prog_id =
    (* prepare database/filesystem -- check dependencies (bowtie) *)
    (* call pbs *)
    (* monitor execution, store intermediate states *)
    (* get and store results *)
    ()

  (* Client or Server *)
  let access_state prog_id =
    "current state / results"



end


module Runtime = DSL.Runtime.Simulation_backend
let runtime = Runtime.create ()

let test name p =
  printf "=== Program %S ===\n" name;
  DSL.print_program ~indent:2 p;
  printf "  Type Checking:\n";
  List.iter ~f:(fun (n, res) ->
    printf "    %s : %s\n" n 
      (match res with
      | Ok t -> DSL.string_of_type t
      | Error b -> b);
  ) (DSL.Verify.type_check_program p);
  begin match Runtime.load_program runtime name p with
  | Ok () -> printf "  Program loaded\n"
  | Error s -> printf "  Program can't be loaded:\n    %s\n"
    (Runtime.string_of_error s)
  end;
  ()


let () =
  System.cmd "echo 'This is a Test File' `date` > /tmp/testfile1" |> ignore;
  System.cmd "echo 'This is a Test File used Twice' `date` > /tmp/fileusedtwice" |> ignore;

  let open DSL.Construct in 
  test "good" [
    "myfile", (file "/tmp/testfile1");
    "bowtie", (bowtie (load_fastq (var "myfile")) 42);
    "rebowtie", (bowtie (var "bowtie") 51);
    "one_int", (int 17);
    "one_string", (str "Hi, World.");
  ];
  test "bad" [
    "wrong_name", (var "__two_underscores");
    "wrong_name2", (var "spécial");
    "wrong_name3'", (int 42);
    "wrong_name4", (ext_int ["good"; "__bad"]);
    "wrong_file", load_fastq (int 42);
    "wrong_var", var "nope";
    "bad_bowtie", (bowtie (var "bowtie") 51);
  ];
  test "with_wrong_external" [
    "wrong_external", (ext_int ["good"; "myfile"])
  ];
  test "with_wrong_file" [
    "wrong_file", (file "/some/random/file")
  ];
  test "optimizable" [
    "bowtie", (bowtie (load_fastq (file "/tmp/fileusedtwice")) 42);
    "rebowtie", (bowtie (load_fastq (file "/tmp/fileusedtwice")) 51);
  ];
  test "good" [ "myfile", (int 42) ];
  test "good" [ "added_value", (ext_fastq ["optimizable"; "bowtie"]) ];
  test "good" [];
  printf "===== Current Runtime =====\n";
  Runtime.print_runtime ~indent:2 runtime;

  let filter = [ "good" ] in
  printf "===== Compiling %s/* stuff =====\n" (Path.str filter);
  Runtime.compile_runtime ~filter runtime;
  printf "=== Current Runtime:\n";
  Runtime.print_runtime ~indent:2 runtime;
  printf "===== Compiling all the stuff =====\n";
  Runtime.compile_runtime runtime;
  printf "=== Current Runtime:\n";
  Runtime.print_runtime ~indent:2 runtime;

printf "----------------------------------------\n";
Runtime.run_program runtime ["good"; "rebowtie" ];

  ()
    
