# Unreleased

## Added

- Added `Index_unix.Syscalls`, a module exposing various Unix bindings for
  interacting with file-systems.

## Fixed

- Fail when `Index_unix.IO` file version number is not as expected.
- Fix a bug when `filter` is called before the first `merge`.

# 1.2.0 (2020-02-25)

## Added

- Added `filter`, removing bindings depending on a predicate (#165)

## Changed

- Parameterise `Index.Make` over arbitrary mutex and thread implementations
  (and remove the obligation for `IO` to provide this functionality). (#160,
  #161)

# 1.1.0 (2019-12-21)

## Changed

- Improve the cooperativeness of the `merge` operation, allowing concurrent read
  operations to share CPU resources with ongoing merges. (#152)

- Improve speed of read operations for read-only instances. (#141)

## Removed

- Remove `force_merge` from `Index.S`, due to difficulties with guaranteeing
  sensible semantics to this function under MRSW access patterns. (#147, #150)

# 1.0.1 (2019-11-29)

## Added

- Provide a better CLI interface for the benchmarks (#130, #133)

## Fixed

- Fix a segmentation fault when using musl <= 1.1.20 by not allocating 64k-byte
  buffers on the thread stack (#132)
- Do not call `pwrite` with `len=0` (#131)
- Clear `log.mem` on `close` (#135)
- Load `log_async` on startup (#136)

# 1.0.0 (2019-11-14)

First stable release.
