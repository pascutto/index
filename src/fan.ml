(* The MIT License

Copyright (c) 2019 Craig Ferguson <craig@tarides.com>
                   Thomas Gazagnaire <thomas@tarides.com>
                   Ioana Cristescu <ioana@tarides.com>
                   Clément Pascutto <clement@tarides.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software. *)

type 'a t = { fans : int64 array; mask : int; shift : int }

let equal t t' =
  let rec loop i =
    if i >= Array.length t.fans then true
    else if Int64.equal t.fans.(i) t'.fans.(i) then loop (i + 1)
    else false
  in
  t.mask = t'.mask
  && t.shift = t'.shift
  && Array.length t.fans = Array.length t'.fans
  && loop 0

let log2 a = log a /. log 2.

let v ~hash_size ~entry_size n =
  let entry_sizef = float_of_int entry_size in
  let entries_per_page = 4096. /. entry_sizef in
  let raw_nb_fans = float_of_int n /. entries_per_page in
  let size = max 0 (int_of_float (ceil (log2 raw_nb_fans))) in
  let nb_fans = 1 lsl size in
  let shift = hash_size - size in
  { fans = Array.make nb_fans 0L; mask = (nb_fans - 1) lsl shift; shift }

let nb_fans t = Array.length t.fans

let fan t h = (h land t.mask) lsr t.shift

let search t h =
  let fan = fan t h in
  let low = if fan = 0 then 0L else t.fans.(fan - 1) in
  (low, t.fans.(fan))

let update t hash off =
  let fan = fan t hash in
  t.fans.(fan) <- off

let finalize t =
  let rec loop curr i =
    if i = Array.length t.fans then ()
    else (
      if t.fans.(i) = 0L then t.fans.(i) <- curr;
      loop t.fans.(i) (i + 1))
  in
  loop 0L 0;
  (t :> [ `Read ] t)

external set_64 : Bytes.t -> int -> int64 -> unit = "%caml_string_set64u"

external get_64 : string -> int -> int64 = "%caml_string_get64"

external swap64 : int64 -> int64 = "%bswap_int64"

let encode_int64 t buf ~off =
  if not Sys.big_endian then set_64 buf off (swap64 t) else set_64 buf off t

let decode_int64 buf ~off =
  if not Sys.big_endian then swap64 (get_64 buf off) else get_64 buf off

let exported_size t = Array.length t.fans * 8

let export t =
  let encoded_size = exported_size t in
  let buf = Bytes.create encoded_size in
  let rec loop i =
    if i >= Array.length t.fans then ()
    else (
      encode_int64 t.fans.(i) buf ~off:(i * 8);
      loop (i + 1))
  in
  loop 0;
  Bytes.unsafe_to_string buf

let import ~hash_size buf =
  let nb_fans = String.length buf / 8 in
  let fans = Array.init nb_fans (fun i -> decode_int64 buf ~off:(i * 8)) in
  let size = int_of_float (log2 (float_of_int nb_fans)) in
  let shift = hash_size - size in
  let mask = (nb_fans - 1) lsl shift in
  { fans; mask; shift }
