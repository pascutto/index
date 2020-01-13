module Stats = Index.Stats

let src =
  let open Metrics in
  let open Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "bytes_read" t.bytes_read;
        int "bytes_written" t.bytes_written;
        int "merge" t.nb_merge;
        int "replace" t.nb_replace;
      ]
  in
  Src.v "bench" ~tags ~data

let add_metrics =
  let no_tags x = x in
  fun () -> Metrics.add src no_tags (fun m -> m (Stats.get ()))

let ( // ) = Filename.concat

module Config = struct
  type t = {
    key_size : int;
    value_size : int;
    with_metrics : bool;
    seed : int;
    data_dir : string;
    nb_entries : int;
    log_size : int;
  }
  [@@deriving yojson]

  let _pp fmt t =
    Format.fprintf fmt
      "Size of keys: %d bytes.@\n\
       Size of values: %d bytes.@\n\
       Number of bindings: %d.@n@\n\
       Log size: %d.@\n\
       Initializing data with seed %d." t.key_size t.value_size t.nb_entries
      t.log_size t.seed

  let _pp_json fmt t = Yojson.Safe.pretty_print fmt (to_yojson t)
end

let with_stats f =
  Stats.reset_stats ();
  let t0 = Sys.time () in
  let x = f () in
  let t1 = Sys.time () -. t0 in
  let stats = Stats.get () in
  (x, t1, stats)

module Benchmark = struct
  type result = {
    time : float;
    ops_per_sec : float;
    mbs_per_sec : float;
    read_amplification_calls : float;
    read_amplification_size : float;
    write_amplification_calls : float;
    write_amplification_size : float;
  }
  [@@deriving yojson]

  let run ~(config : Config.t) (f : config:Config.t -> unit -> unit) =
    let _, time, stats = with_stats (fun () -> f ~config ()) in
    let nb_entriesf = Float.of_int config.nb_entries in
    let entry_sizef = Float.of_int (config.key_size + config.value_size) in
    let read_amplification_size =
      Float.of_int stats.bytes_read /. (entry_sizef *. nb_entriesf)
    in
    let read_amplification_calls = Float.of_int stats.nb_reads /. nb_entriesf in
    let write_amplification_size =
      Float.of_int stats.bytes_written /. (entry_sizef *. nb_entriesf)
    in
    let write_amplification_calls =
      Float.of_int stats.nb_writes /. nb_entriesf
    in
    let ops_per_sec = nb_entriesf /. time in
    let mbs_per_sec = entry_sizef *. nb_entriesf /. 1_048_576. /. time in
    {
      time;
      ops_per_sec;
      mbs_per_sec;
      read_amplification_calls;
      read_amplification_size;
      write_amplification_calls;
      write_amplification_size;
    }

  let pp_result fmt result =
    Format.fprintf fmt
      "Total time: %f@\n\
       Operations per second: %f@\n\
       Mbytes per second: %f@\n\
       Read amplification in syscalls: %f@\n\
       Read amplification in bytes: %f@\n\
       Write amplification in syscalls: %f@\n\
       Write amplification in bytes: %f" result.time result.ops_per_sec
      result.mbs_per_sec result.read_amplification_calls
      result.read_amplification_size result.write_amplification_calls
      result.write_amplification_size

  let _pp_result_json fmt result =
    Yojson.Safe.pretty_print fmt (result_to_yojson result)
end

module type Key = sig
  include Index.Key

  val v : unit -> t

  val compare : t -> t -> int
end

module type Value = sig
  include Index.Value

  val v : unit -> t

  val compare : t -> t -> int
end

module type Bench = sig
  module Index : Index.S

  type key

  type value

  val make_bindings_pool : config:Config.t -> (key * value) array

  val bindings_pool : (key * value) array ref

  val absent_bindings_pool : (key * value) array ref

  val run :
    config:Config.t ->
    root:string ->
    name:string ->
    (config:Config.t ->
    (fresh:bool -> readonly:bool -> string -> Index.t) ->
    unit ->
    unit) ->
    Benchmark.result

  type suite_elt = {
    name : string;
    synopsis : string;
    benchmark :
      config:Config.t ->
      (fresh:bool -> readonly:bool -> string -> Index.t) ->
      unit ->
      unit;
    dependency : string option;
  }

  val suite : suite_elt list

  val pp_suite : Format.formatter -> suite_elt list -> unit

  val schedule : (string -> bool) -> suite_elt list -> suite_elt list
end

module Index_bench (Key : Key) (Value : Value) : Bench = struct
  module Index = Index_unix.Make (Key) (Value)

  type key = Key.t

  type value = Value.t

  let make_bindings_pool ~(config : Config.t) =
    Array.init config.nb_entries (fun _ ->
        let k = Key.v () in
        let v = Value.v () in
        (k, v))

  let bindings_pool = ref [||]

  let absent_bindings_pool = ref [||]

  let write ~(config : Config.t) ?(with_flush = false) bindings rw =
    Array.iter
      (fun (k, v) ->
        Index.replace rw k v;
        if with_flush then Index.flush rw;
        if config.with_metrics then add_metrics ())
      bindings

  let read ~(config : Config.t) bindings r =
    Array.iter
      (fun (k, _) ->
        ignore (Index.find r k);
        if config.with_metrics then add_metrics ())
      bindings

  let read_absent ~(config : Config.t) bindings r =
    Array.iter
      (fun (k, _) ->
        try
          ignore (Index.find r k);
          failwith "Absent value found"
        with Not_found -> if config.with_metrics then add_metrics ())
      bindings

  let write_random ~config v () =
    v ~fresh:true ~readonly:false "" |> write ~config !bindings_pool

  let write_seq ~config v =
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> Key.compare (fst a) (fst b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~config bindings

  let write_seq_hash ~config v =
    let hash e = Hashtbl.hash (fst e) in
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> Int.compare (hash a) (hash b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~config bindings

  let write_rev_seq_hash ~config v =
    let hash e = Hashtbl.hash (fst e) in
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> -Int.compare (hash a) (hash b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~config bindings

  let write_sync ~config v () =
    v ~fresh:true ~readonly:false ""
    |> write ~config ~with_flush:true !bindings_pool

  let iter ~(config : Config.t) v () =
    v ~fresh:false ~readonly:true ""
    |> Index.iter (fun _ _ -> if config.with_metrics then add_metrics ())

  let find_random ~config v () =
    v ~fresh:false ~readonly:false "" |> read ~config !bindings_pool

  let find_random_ro ~config v () =
    v ~fresh:false ~readonly:true "" |> read ~config !bindings_pool

  let find_absent ~config v () =
    v ~fresh:false ~readonly:false ""
    |> read_absent ~config !absent_bindings_pool

  let find_absent_ro ~config v () =
    v ~fresh:false ~readonly:true ""
    |> read_absent ~config !absent_bindings_pool

  let run ~(config : Config.t) ~root ~name
      (b :
        config:Config.t ->
        (fresh:bool -> readonly:bool -> string -> Index.t) ->
        unit ->
        unit) =
    let indices = ref [] in
    let index_v ~fresh ~readonly n =
      let v =
        Index.v ~fresh ~readonly ~log_size:config.log_size (root // name // n)
      in
      indices := v :: !indices;
      v
    in
    let result = Benchmark.run ~config (b index_v) in
    !indices |> List.iter (fun i -> Index.close i);
    result

  type suite_elt = {
    name : string;
    synopsis : string;
    benchmark :
      config:Config.t ->
      (fresh:bool -> readonly:bool -> string -> Index.t) ->
      unit ->
      unit;
    dependency : string option;
  }

  let suite =
    [
      {
        name = "replace_random";
        synopsis = "Replace in random order";
        benchmark = write_random;
        dependency = None;
      };
      {
        name = "replace_random_sync";
        synopsis = "Replace in random order with sync";
        benchmark = write_sync;
        dependency = None;
      };
      {
        name = "replace_increasing_keys";
        synopsis = "Replace in increasing order of keys";
        benchmark = write_seq;
        dependency = None;
      };
      {
        name = "replace_increasing_hash";
        synopsis = "Replace in increasing order of hash";
        benchmark = write_seq_hash;
        dependency = None;
      };
      {
        name = "replace_decreasing_hash";
        synopsis = "Replace in decreasing order of hashes";
        benchmark = write_rev_seq_hash;
        dependency = None;
      };
      {
        name = "iter_rw";
        synopsis = "[RW] Iter";
        benchmark = iter;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_ro";
        synopsis = "[RO] Find in random order";
        benchmark = find_random_ro;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_rw";
        synopsis = "[RW] Find in random order";
        benchmark = find_random;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_ro";
        synopsis = "[RO] Find absent values";
        benchmark = find_absent_ro;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_rw";
        synopsis = "[RW] Find absent values";
        benchmark = find_absent;
        dependency = Some "replace_random";
      };
    ]

  let pp_suite fmt suite =
    let pp_bench ppf b = Fmt.pf ppf "%s\t-- %s" b.name b.synopsis in
    Format.fprintf fmt "%a" Fmt.(list ~sep:(const string "\n") pp_bench) suite

  let schedule p s =
    let todos = List.map fst in
    let init = ref (s |> List.map (fun b -> (p b.name, b))) in
    let apply_dep s =
      let deps =
        s
        |> List.fold_left
             (fun acc (todo, b) ->
               if todo then
                 match b.dependency with Some s -> s :: acc | None -> acc
               else acc)
             []
      in
      s |> List.map (fun (todo, b) -> (todo || List.mem b.name deps, b))
    in
    let next = ref (apply_dep !init) in
    while todos !init <> todos !next do
      init := !next;
      next := apply_dep !init
    done;
    let r = List.filter fst !init |> List.map snd in
    r
end

let _list_benchs (module Bench : Bench) () =
  Format.printf "%a" Bench.pp_suite Bench.suite

let random_string string_size =
  let random_char () = char_of_int (33 + Random.int 94) in
  String.init string_size (fun _i -> random_char ())

let make_bench (config : Config.t) =
  let key =
    ( module struct
      type t = string

      let v () = random_string config.key_size

      let compare = String.compare

      let hash = Hashtbl.hash

      let hash_size = 30

      let encode s = s

      let decode s off = String.sub s off config.key_size

      let encoded_size = config.key_size

      let equal = String.equal

      let pp s = Fmt.fmt "%s" s
    end : Key )
  in
  let value =
    ( module struct
      type t = string

      let v () = random_string config.value_size

      let compare = String.compare

      let encode s = s

      let decode s off = String.sub s off config.value_size

      let encoded_size = config.value_size

      let pp s = Fmt.fmt "%s" s
    end : Value )
  in
  (module Index_bench ((val key)) ((val value)) : Bench)

let init (config : Config.t) =
  Random.init config.seed;
  if config.with_metrics then (
    Metrics.enable_all ();
    Metrics_gnuplot.set_reporter ();
    Metrics_unix.monitor_gc 0.1 );
  config |> make_bench

let run filter key_size value_size with_metrics seed data_dir nb_entries
    log_size =
  let config =
    Config.
      {
        key_size;
        value_size;
        with_metrics;
        seed;
        data_dir;
        nb_entries;
        log_size;
      }
  in
  let name_filter =
    match filter with None -> fun _ -> true | Some re -> Re.execp re
  in
  let module Bench = (val init config) in
  Bench.suite
  |> Bench.schedule name_filter
  |> List.iter (fun Bench.{ synopsis; name; benchmark; dependency } ->
         let name = match dependency with None -> name | Some name -> name in
         let result = Bench.run ~config ~root:data_dir ~name benchmark in
         Logs.app (fun l ->
             l "%s@\n    @[%a@]@\n" synopsis Benchmark.pp_result result))

open Cmdliner

let regex =
  let parse s =
    try Ok Re.(compile @@ Pcre.re s) with
    | Re.Perl.Parse_error -> Error (`Msg "Perl-compatible regexp parse error")
    | Re.Perl.Not_supported -> Error (`Msg "unsupported regexp feature")
  in
  let print = Re.pp_re in
  Arg.conv (parse, print)

let name_filter =
  let doc = "A regular expression matching the names of benchmarks to run" in
  Arg.(
    value
    & opt (some regex) None
    & info [ "f"; "filter" ] ~doc ~docv:"NAME_REGEX")

let data_dir =
  let doc = "Set directory for the data files" in
  Arg.(value & opt dir "_bench" & info [ "d"; "directory" ] ~doc)

let key_size =
  let doc = "Sets the size of the keys." in
  Arg.(value & opt int 32 & info [ "key_size" ] ~doc)

let value_size =
  let doc = "Sets the size of the values." in
  Arg.(value & opt int 13 & info [ "value_size" ] ~doc)

let seed =
  let doc = "The seed used to generate random data." in
  Arg.(value & opt int 0 & info [ "s"; "seed" ] ~doc)

let with_metrics =
  let doc = "Use Metrics; note that it has an impact on performance" in
  Arg.(value & flag & info [ "m"; "with_metrics" ] ~doc)

let nb_entries =
  let doc = "Sets the number of bindings." in
  Arg.(value & opt int 10_000_000 & info [ "entries" ] ~doc)

let log_size =
  let doc = "Sets the log size." in
  Arg.(value & opt int 500_000 & info [ "log_size" ] ~doc)

(* let list_cmd =
  let doc = "List all available benchmarks." in
  (Term.(pure list_benchs $ const ()), Term.info "list" ~doc) *)

let cmd =
  let doc = "Run all the benchmarks." in
  ( Term.(
      const run
      $ name_filter
      $ key_size
      $ value_size
      $ with_metrics
      $ seed
      $ data_dir
      $ nb_entries
      $ log_size),
    Term.info "run" ~doc ~exits:Term.default_exits )

let () =
  let choices = [ (* list_cmd *) ] in
  Term.(exit @@ eval_choice cmd choices)
