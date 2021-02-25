let ( ++ ) = Int64.add

type t = {
  buffer : bytes;
  mutable buffer_offset : int;
  mutable raw : Raw.t;
  mutable flushed : int64;
  flush_callback : (unit -> unit) option;
}

let create ~flush_callback ~flushed raw n =
  let s = Bytes.create n in
  { buffer = s; buffer_offset = 0; raw; flushed; flush_callback }

let is_empty b = b.buffer_offset = 0

let clear b = b.buffer_offset <- 0

let flush ?(with_fsync = false) t =
  (if is_empty t then ()
  else
    let buf = Bytes.unsafe_to_string t.buffer in
    Option.iter (fun f -> f ()) t.flush_callback;
    Raw.unsafe_write t.raw ~off:t.flushed buf 0 t.buffer_offset;
    t.flushed <- t.flushed ++ Int64.of_int (String.length buf));
  if with_fsync then Raw.fsync t.raw

let add_substring b s offset len =
  let new_off =
    let n = b.buffer_offset + len in
    if n <= Bytes.length b.buffer then n
    else (
      flush b;
      len)
  in
  Bytes.unsafe_blit_string s offset b.buffer b.buffer_offset len;
  b.buffer_offset <- new_off

let add_subbytes b s offset len =
  add_substring b (Bytes.unsafe_to_string s) offset len

let add_string b s =
  let len = String.length s in
  let new_off =
    let n = b.buffer_offset + len in
    if n <= Bytes.length b.buffer then n
    else (
      flush b;
      len)
  in
  Bytes.unsafe_blit_string s 0 b.buffer b.buffer_offset len;
  b.buffer_offset <- new_off

let add_bytes b s = add_string b (Bytes.unsafe_to_string s)
