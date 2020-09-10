let () =
  Alcotest.run "index"
    [
      ("cache", Cache.tests);
      ("search", Search.tests);
      ("external sort", External_sort.tests);
    ]
