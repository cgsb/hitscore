all: build

GEN_DEPS=bin/sexp2db.ml data/hitscore_layout

src/lib/hitscore_db_access.ml: $(GEN_DEPS)
	./bin/sexp2db.ml codegen data/hitscore_layout src/lib/hitscore_db_access.ml

build: src/lib/hitscore_db_access.ml
	ocaml setup.ml -build

install:
	ocaml setup.ml -reinstall

uninstall:
	ocaml setup.ml -uninstall

doc:
	ocaml setup.ml -doc

clean:
	ocaml setup.ml -clean


# clean everything and uninstall
fresh: clean uninstall

.PHONY: doc install uninstall clean fresh
