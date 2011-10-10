open Printf



module DSL = struct

  module Definition = struct
    type fastq = {
      fastq_comment : string;
      fastq_record : string;
      fastq_quality: string;
    }

    type atom =
      | Int of int
      | String of string
      | Fastq of fastq list
      | File of string * string (* path × md5 *)

    type expression =
      | Constant of atom
      | Variable of string
      | Load_fastq of expression
      | Bowtie of expression * expression (* fastq x some_other -> ??? *)

    type global = string * expression

    type program = global list

  end

  open Definition

  module Construct = struct
      
    let int i = Constant (Int i)
    let str s = Constant (String s)
    let var s = Variable s
    let file f = Constant (File (f, (* md5 *) f))
    let load_fastq f = Load_fastq f 
    let bowtie e i = Bowtie (e, int i)

  end

  module Runtime = struct

    (* Representation of the run-time in the meta-language *)
    module Simulation_backend = struct

      type not_yet_defined


      (* A "loaded" program, must be type-checked *)
      type compilation_result = not_yet_defined
      
      type runtime_program = {
        
        (* An Id which must be unique *)
        program_id: string;
        original_program: program;
        
        (* A Stack of results of compilation passes *)
        mutable compilations: compilation_result list; 
      }

      type runtime_int = {
        (* kind of pointer / unique id *)
        int_id: string;
        (* again a stack, representing history *)
        mutable int_history: int list;
      }

      (* ... *)

      type runtime_value = 
        | Int of runtime_int

      type meta = {
        mutable programs: (string, runtime_program) Hashtbl.t;
        mutable values: (string, runtime_value) Hashtbl.t;
      }
    end

  end


  let string_of_expression e =
    let fastq = fun { fastq_comment; fastq_record; fastq_quality } ->
      sprintf "(%s %s %s)" fastq_comment fastq_record fastq_quality in
    let basic = function
      | Int i -> sprintf "%d" i
      | String s -> sprintf "%S" s
      | Fastq f -> 
        sprintf "(fastq: %s)" (String.concat ", " (List.map fastq f))
      | File (f, _) -> sprintf "(file: %s)" f in
    let rec expr = function
      | Constant c -> sprintf "(cst: %s)" (basic c)
      | Variable v -> sprintf "(var: %s)" v
      | Load_fastq e -> sprintf "(load_fastq: %s)" (expr e)
      | Bowtie (e1, e2) -> sprintf "(bowtie %s %s)" (expr e1) (expr e2)
    in
    (expr e)

  let print_program =
    List.iter (fun (g, e) -> printf "[%S:\n  %s\n]\n" g (string_of_expression e))

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
  let compile_program prog_id =
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

let () =
  let p =
    let open DSL.Construct in [
      "myfile", (file "/path/to/myfile.fastq");
      "bowtie", (bowtie (load_fastq (var "myfile")) 42);
      "rebowtie", (bowtie (var "bowtie") 51)
    ] in
  DSL.print_program p


