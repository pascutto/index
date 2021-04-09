exception Invalid_size of string
(** The exception raised when trying to encode a key or a value of size other
    than encoded_size *)

module type Key = sig
  type t [@@deriving repr]
  (** The type for keys. *)

  val equal : t -> t -> bool
  (** The equality function for keys. *)

  type metric

  val metric : t -> metric

  val compare : metric -> metric -> int

  val encode : t -> string
  (** [encode] is an encoding function. The resultant encoded values must have
      size {!encoded_size}. *)

  val encoded_size : int
  (** [encoded_size] is the size of the result of {!encode}, expressed in number
      of bytes. *)

  val decode : string -> int -> t
  (** [decode s off] is the decoded form of the encoded value at the offset
      [off] of string [s]. Must satisfy [decode (encode t) 0 = t]. *)
end

module type Value = sig
  type t [@@deriving repr]

  val encode : t -> string

  val encoded_size : int

  val decode : string -> int -> t
end

module Entry = struct
  module type S = sig
    type key

    type value

    type t = private { key : key; value : value } [@@deriving repr]

    val v : key -> value -> t

    val encoded_size : int

    val decode : string -> int -> t

    val decode_key : string -> int -> key

    val decode_value : string -> int -> value

    val encode : t -> (string -> unit) -> unit

    val encode' : key -> value -> (string -> unit) -> unit
  end

  module Make (K : Key) (V : Value) :
    S with type key := K.t and type value := V.t = struct
    type t = { key : K.t; value : V.t } [@@deriving repr]

    let v key value = { key; value }

    let encoded_size = K.encoded_size + V.encoded_size

    let decode string off =
      let key = K.decode string off in
      let value = V.decode string (off + K.encoded_size) in
      { key; value }

    let decode_key string off =
      let key = K.decode string off in
      key

    let decode_value string off = V.decode string (off + K.encoded_size)

    let encode' key value f =
      let encoded_key = K.encode key in
      let encoded_value = V.encode value in
      if String.length encoded_key <> K.encoded_size then
        raise (Invalid_size encoded_key);
      if String.length encoded_value <> V.encoded_size then
        raise (Invalid_size encoded_value);
      f encoded_key;
      f encoded_value

    let encode { key; value; _ } f = encode' key value f
  end
end

module String_fixed (L : sig
  val length : int
end) : sig
  type t = string

  include Key with type t := string

  include Value with type t := string
end = struct
  type t = string [@@deriving repr]

  let encode s = s

  let decode s off = String.sub s off L.length

  let encoded_size = L.length

  let equal = String.equal
end
