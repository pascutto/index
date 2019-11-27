(** Benchmarks *)

open Common

let src = Logs.Src.create "db_bench"

module Log = (val Logs.src_log src : Logs.LOG)

let seed = 1

let () = Random.init seed

let key_size = 32

let hash_size = 30

let value_size = 13

let nb_entries = 10_000_000

let log_size = 500_000

let ( // ) = Filename.concat

module Context = Make_context (struct
  let key_size = key_size

  let hash_size = hash_size

  let value_size = value_size
end)

let entry_size = key_size + value_size

let random = Array.make nb_entries ("", "")

let populate () =
  let rec loop i =
    if i = nb_entries then ()
    else
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      let () = random.(i) <- (k, v) in
      loop (i + 1)
  in
  loop 0

let print_results db f nb_entries =
  let _, time = with_timer f in
  let micros = time *. 1_000_000. in
  let sec_op = micros /. float_of_int nb_entries in
  let mb = float_of_int (entry_size * nb_entries / 1_000_000) /. time in
  let ops_sec = float_of_int nb_entries /. time in
  Log.app (fun l ->
      l "%s: %f micros/op; \t %f op/s; \t %f MB/s; \t total time = %fs." db
        sec_op ops_sec mb time)

let rec random_new_key ar =
  let k = Context.Key.v () in
  if Array.exists (fun (k', _) -> k = k') ar then random_new_key ar else k

let populate_absents ar nb_entries =
  let absents = Array.make nb_entries ("", "") in
  let v = Context.Value.v () in
  let rec loop i =
    if i = nb_entries then absents
    else
      let k = random_new_key ar in
      let () = absents.(i) <- (k, v) in
      loop (i + 1)
  in
  loop 0

module Index = struct
  module Index = Index_unix.Make (Context.Key) (Context.Value)

  let root = "_bench" // "db_bench"

  let print_results = print_results "index"

  let write_amplif () =
    let stats = Index_unix.get_stats () in
    let ratio_bytes =
      float_of_int stats.bytes_written /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads = float_of_int stats.nb_writes /. float_of_int nb_entries in
    Log.app (fun l ->
        l "\twrite amplification in bytes = %f; in nb of writes = %f; "
          ratio_bytes ratio_reads)

  let read_amplif nb_entries =
    let stats = Index_unix.get_stats () in
    let ratio_bytes =
      float_of_int stats.bytes_read /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads = float_of_int stats.nb_reads /. float_of_int nb_entries in
    Log.app (fun l ->
        l "\tread amplification in bytes = %f; in nb of reads = %f " ratio_bytes
          ratio_reads)

  let init () =
    if Sys.file_exists root then (
      let cmd = Printf.sprintf "rm -rf %s" root in
      Logs.app (fun l -> l "exec: %s\n" cmd);
      let _ = Sys.command cmd in
      () )

  let write rw () = Array.iter (fun (k, v) -> Index.replace rw k v) random

  let read r () = Array.iter (fun (k, _) -> ignore (Index.find r k)) random

  let write_random () =
    Index_unix.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_random") in
    print_results (write rw) nb_entries;
    write_amplif ();
    rw

  let write_seq () =
    Index_unix.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq") in
    Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
    print_results (write rw) nb_entries;
    write_amplif ();
    Index.close rw

  let write_seq_hash () =
    Index_unix.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq_hash") in
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> compare (hash a) (hash b)) random;
    print_results (write rw) nb_entries;
    write_amplif ();
    Index.close rw

  let write_rev_seq_hash () =
    Index_unix.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_rev_seq_hash") in
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> compare (hash a) (hash b)) random;
    let write rw =
      Array.fold_right (fun (k, v) () -> Index.replace rw k v) random
    in
    print_results (write rw) nb_entries;
    write_amplif ();
    Index.close rw

  let write_sync () =
    Index_unix.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_sync") in
    let write rw () =
      Array.iter
        (fun (k, v) ->
          Index.replace rw k v;
          Index.flush rw)
        random
    in
    print_results (write rw) nb_entries;
    write_amplif ();
    Index.close rw

  let overwrite rw =
    Index_unix.reset_stats ();
    print_results (write rw) nb_entries;
    write_amplif ()

  let read_random r =
    Index_unix.reset_stats ();
    print_results (read r) nb_entries;
    read_amplif nb_entries

  let ro_read_random rw =
    Index.flush rw;
    Index_unix.reset_stats ();
    let ro =
      Index.v ~fresh:false ~readonly:true ~log_size (root // "fill_random")
    in
    print_results (read ro) nb_entries;
    read_amplif nb_entries

  let read_seq r =
    Index_unix.reset_stats ();
    let read () = Index.iter (fun _ _ -> ()) r in
    print_results read nb_entries;
    read_amplif nb_entries

  let read_absent r =
    let absents = populate_absents random 1000 in
    let read r () =
      Array.iter
        (fun (k, _) -> try ignore (Index.find r k) with Not_found -> ())
        absents
    in
    Index_unix.reset_stats ();
    print_results (read r) 1000;
    read_amplif 1000

  let close rw = Index.close rw
end

let init () =
  Common.report ();
  Index.init ();
  Log.app (fun l -> l "Keys: %d bytes each." key_size);
  Log.app (fun l -> l "Values: %d bytes each." value_size);
  Log.app (fun l -> l "Entries: %d." nb_entries);
  Log.app (fun l -> l "Log size: %d." log_size);
  populate ()

open Cmdliner

let run input =
  init ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order");
  let rw = Index.write_random () in
  let () =
    match input with
    | `Find `RW | `IndexAll | `All | `Minimal ->
        let () = Log.app (fun l -> l "\n RW Read in random order") in
        Index.read_random rw
    | _ -> ()
  in
  let () =
    match input with
    | `Find `RO | `IndexAll | `All | `Minimal ->
        let () = Log.app (fun l -> l "\n RO Read in random order") in
        Index.ro_read_random rw
    | _ -> ()
  in
  let () =
    match input with
    | `Find `Absent | `IndexAll | `All | `Minimal ->
        let () = Log.app (fun l -> l "\n Read 1000 absent values") in
        Index.read_absent rw
    | _ -> ()
  in
  let () =
    match input with
    | `Find `Seq | `IndexAll | `All ->
        let () =
          Log.app (fun l ->
              l
                "\n\
                 Read in sequential order (increasing order of hashes for \
                 index, increasing order of keys for lmdb)")
        in
        Index.read_seq rw
    | _ -> ()
  in
  let () =
    match input with
    | `Write `IncKey | `All ->
        let () = Log.app (fun l -> l "\n Fill in increasing order of keys") in
        Index.write_seq ()
    | _ -> ()
  in
  let () =
    match input with
    | `Write `IncHash | `All ->
        let () =
          Log.app (fun l -> l "\n Fill in increasing order of hashes")
        in
        Index.write_seq_hash ()
    | _ -> ()
  in
  let () =
    match input with
    | `Write `DecHash | `All ->
        let () =
          Log.app (fun l -> l "\n Fill in decreasing order of hashes")
        in
        Index.write_rev_seq_hash ()
    | _ -> ()
  in
  let () =
    match input with
    | `Write `Sync | `All ->
        let () =
          Log.app (fun l ->
              l "\n Fill in random order and sync after each write")
        in
        Index.write_sync ()
    | _ -> ()
  in
  let () =
    match input with
    | `OverWrite | `All ->
        let () = Log.app (fun l -> l "\n Overwrite") in
        Index.overwrite rw
    | _ -> ()
  in
  Index.close rw

let input =
  let doc =
    "Select which benchmark(s) to run. Available options are: `write`, \
     `write-keys`, `write-hashes`, `write-dec`, `find-rw`, `find-ro` , \
     `find-seq`,  `find-absent`, `overwrite`, `minimal`or `all`. Default \
     option is `minimal`"
  in
  let options =
    Arg.enum
      [
        ("all", `All);
        ("minimal", `Minimal);
        ("find-rw", `Find `RW);
        ("find-ro", `Find `RO);
        ("find-seq", `Find `Seq);
        ("find-absent", `Find `Absent);
        ("write-keys", `Write `IncKey);
        ("write-hashes", `Write `IncHash);
        ("write-dec", `Write `DecHash);
        ("write-sync", `Write `Sync);
        ("overwrite", `OverWrite);
      ]
  in
  Arg.(value & opt options `Minimal & info [ "b"; "bench" ] ~doc)

let cmd =
  let doc = "Specify the benchmark you want to run." in
  (Term.(const run $ input), Term.info "run" ~doc ~exits:Term.default_exits)

let () = Term.(exit @@ eval cmd)
