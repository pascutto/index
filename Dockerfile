FROM ocaml/opam2
RUN sudo apt install -y m4 perl libgmp-dev pkg-config
COPY   .  index 

WORKDIR index
RUN opam install -y --deps-only -t .
RUN eval $(opam env)