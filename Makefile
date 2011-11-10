.PHONY: doc install uninstall clean fresh dot update_psql dbinit dbclear dbupdate

all: build

GEN_DEPS=bin/sexp2db.ml data/hitscore_layout

src/lib/hitscore_db_access.ml: $(GEN_DEPS)
	./bin/sexp2db.ml codegen data/hitscore_layout src/lib/hitscore_db_access.ml


_build/hitscore_layout_digraph.dot: data/hitscore_layout ./bin/sexp2db.ml
	./bin/sexp2db.ml digraph data/hitscore_layout $@

hitscore_layout_digraph.pdf: _build/hitscore_layout_digraph.dot
	dot -Tpdf $< -o$@

_build/hitscore_db_digraph.dot: data/hitscore_layout ./bin/sexp2db.ml
	./bin/sexp2db.ml db_digraph data/hitscore_layout $@

hitscore_db_digraph.pdf: _build/hitscore_db_digraph.dot
	dot -Tpdf $< -o$@

dots: hitscore_layout_digraph.pdf hitscore_db_digraph.pdf


update_psql: 
	./bin/sexp2db.ml postgres data/hitscore_layout _build/
dbinit:
	psql -1 -q -f _build/hitscore_layout_init.psql
dbclear:
	psql -1 -q -f _build/hitscore_layout_clear.psql

dbupdate: dbclear update_psql dbinit

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

