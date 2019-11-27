(** Benchmarks *)

open Common
open Rresult

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

module Lmdb = struct
  open Lmdb

  let root = "/tmp"

  let print_results = print_results "lmdb "

  let print_stats (txn, ddb) =
    let stats = R.get_ok (db_stat txn ddb) in
    Log.app (fun l ->
        l
          "psize = %d; depth= %d; branch_pages= %d; leaf_pages= %d; \
           overflow_pages= %d; entries= %d;"
          stats.psize stats.depth stats.branch_pages stats.leaf_pages
          stats.overflow_pages stats.entries)

  let cleanup () =
    let files = [ root // "data.mdb"; root // "lock.mdb" ] in
    ListLabels.iter files ~f:(fun fn -> Sys.(if file_exists fn then remove fn))

  let fail_on_error f =
    match f () with Ok _ -> () | Error err -> failwith (string_of_error err)

  let flags = [ Lmdb.NoRdAhead; Lmdb.NoSync; Lmdb.NoMetaSync; Lmdb.NoTLS ]

  let mapsize = 409_600_000_000L

  let get_wtxn dir flags =
    cleanup ();
    opendir dir ~mapsize ~flags 0o644 >>= fun env ->
    create_rw_txn env >>= fun txn ->
    opendb txn >>= fun ddb -> Ok ((txn, ddb), env)

  let write (txn, ddb) () =
    Array.iter
      (fun (k, v) -> fail_on_error (fun () -> Lmdb.put_string txn ddb k v))
      random

  let read (txn, ddb) () =
    Array.iter
      (fun (k, _) ->
        ignore (Bigstring.to_string (R.get_ok (Lmdb.get txn ddb k))))
      random

  let write_random () =
    get_wtxn root flags >>| fun (rw, env) ->
    print_results (write rw) nb_entries;
    print_stats rw;
    (rw, env)

  let write_seq () =
    Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
    get_wtxn root flags >>| fun (rw, env) ->
    print_results (write rw) nb_entries;
    closedir env

  let write_sync () =
    get_wtxn root [ Lmdb.NoRdAhead ] >>| fun (rw, env) ->
    let write (txn, ddb) env ls () =
      Array.iter
        (fun (k, v) ->
          fail_on_error (fun () ->
              Lmdb.put_string txn ddb k v >>= fun () -> sync env))
        ls
    in
    print_results (write rw env random) nb_entries;
    closedir env

  let overwrite rw = print_results (write rw) nb_entries

  let read_random r = print_results (read r) nb_entries

  (*use a new db, created without the flag Lmdb.NoRdAhead*)
  let read_seq () =
    let rw, env =
      R.get_ok
        ( get_wtxn root [ Lmdb.NoSync; Lmdb.NoMetaSync ] >>| fun (rw, env) ->
          let () = write rw () in
          (rw, env) )
    in
    let read (txn, ddb) () =
      opencursor txn ddb >>= fun cursor ->
      cursor_first cursor >>= fun () ->
      cursor_iter
        ~f:(fun (k, v) ->
          ignore (Bigstring.to_string k);
          ignore (Bigstring.to_string v);
          Ok ())
        cursor
      >>| fun () -> cursor_close cursor
    in
    let aux_read r () = fail_on_error (read r) in
    print_results (aux_read rw) nb_entries;
    closedir env

  let close env = closedir env
end

let init () =
  Common.report ();
  Lmdb.cleanup ();
  Log.app (fun l -> l "Keys: %d bytes each." key_size);
  Log.app (fun l -> l "Values: %d bytes each." value_size);
  Log.app (fun l -> l "Entries: %d." nb_entries);
  Log.app (fun l -> l "Log size: %d." log_size);
  populate ()

open Cmdliner

let run input =
  init ();
  Log.app (fun l -> l "\n Fill in random order");
  let lmdb, env = R.get_ok (Lmdb.write_random ()) in
  let () =
    match input with
    | `Read | `All ->
        let () = Log.app (fun l -> l "\n Read in random order ") in
        Lmdb.read_random lmdb
    | _ -> ()
  in
  let () =
    match input with
    | `ReadSeq | `All ->
        let () =
          Log.app (fun l ->
              l
                "\n\
                 Read in sequential order (increasing order of hashes for \
                 index, increasing order of keys for lmdb)")
        in
        Lmdb.read_seq ()
    | _ -> ()
  in
  let () =
    match input with
    | `OverWrite | `All ->
        let () = Log.app (fun l -> l "\n Overwrite") in
        Lmdb.overwrite lmdb
    | _ -> ()
  in
  let () =
    match input with
    | `WriteSync | `All ->
        let () =
          Log.app (fun l ->
              l "\n Fill in random order and sync after each write")
        in
        Lmdb.fail_on_error Lmdb.write_sync
    | _ -> ()
  in
  let () =
    match input with
    | `WriteSeq | `All ->
        let () = Log.app (fun l -> l "\n Fill in increasing order of keys") in
        Lmdb.fail_on_error Lmdb.write_seq;
        populate ()
    | _ -> ()
  in
  Lmdb.close env

let input =
  let doc =
    "Select which benchmark(s) to run. Available options are: `write`, \
     `write-seq`, `write-sync`, `read`, `read-seq` or `overwrite` or `all`."
  in
  let options =
    Arg.enum
      [
        ("all", `All);
        ("write-seq", `WriteSeq);
        ("write-random", `Write);
        ("write-sync", `WriteSync);
        ("read", `Read);
        ("read-seq", `ReadSeq);
        ("overwrite", `Overwrite);
      ]
  in
  Arg.(value & opt options `All & info [ "b"; "bench" ] ~doc)

let cmd =
  let doc = "Specify the benchmark you want to run." in
  (Term.(const run $ input), Term.info "run" ~doc ~exits:Term.default_exits)

let () = Term.(exit @@ eval cmd)
