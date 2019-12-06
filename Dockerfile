FROM ocaml/opam2
RUN sudo apt install -y m4 perl libgmp-dev pkg-config
COPY  --chown=opam:opam  .  index 

WORKDIR index
RUN opam install -y --deps-only -t .
ADD --chown=opam . .
RUN opam config exec -- make -C .
RUN eval $(opam env)  && \
         dune exec -- bench/main.exe -b all
