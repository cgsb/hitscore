.PHONY: doc install uninstall clean fresh dot update_psql dbinit dbclear dbupdate

all: build

GENERATOR=_build/src/codegen/hitscoregen.native

$(GENERATOR):
	ocamlbuild src/codegen/hitscoregen.native

LAYOUT_SOURCE=data/hitscore_layout

_build/hitscore_layout_digraph.dot: $(LAYOUT_SOURCE) $(GENERATOR)
	$(GENERATOR) digraph $(LAYOUT_SOURCE) $@

_doc/hitscore_layout_digraph.pdf: _build/hitscore_layout_digraph.dot
	dot -Tpdf $< -o$@

_doc/hitscore_layout_digraph.png: _build/hitscore_layout_digraph.dot
	dot -Tpng $< -o$@

_build/hitscore_db_digraph.dot: $(LAYOUT_SOURCE) $(GENERATOR)
	$(GENERATOR) db_digraph $(LAYOUT_SOURCE) $@

_doc/hitscore_db_digraph.pdf: _build/hitscore_db_digraph.dot
	dot -Tpdf $< -o$@

_doc/hitscore_db_digraph.png: _build/hitscore_db_digraph.dot
	dot -Tpng $< -o$@

_doc/:
	mkdir _doc

dots: _doc/hitscore_layout_digraph.pdf _doc/hitscore_db_digraph.pdf


update_psql: $(GENERATOR)
	$(GENERATOR) postgres $(LAYOUT_SOURCE) _build/
dbinit:
	psql -1 -q -f _build/hitscore_layout_init.psql
dbclear:
	psql -1 -q -f _build/hitscore_layout_clear.psql

dbupdate: dbclear update_psql dbinit

build:
	ocaml setup.ml -build

install:
	ocaml setup.ml -reinstall

uninstall:
	ocaml setup.ml -uninstall

libdoc:
	ocaml setup.ml -doc

customdoc: _doc/hitscore_layout_digraph.png _doc/hitscore_db_digraph.png

doc: libdoc customdoc
	mkdir -p _doc/lib
	cp hitscoredoc.docdir/* _doc/lib/

clean:
	ocaml setup.ml -clean


# clean everything and uninstall
fresh: clean uninstall

# clean setup files, rebuilding may require additional tools
distclean: fresh
	ocaml setup.ml -distclean
	oasis setup-clean
