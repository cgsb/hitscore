.PHONY: doc install uninstall clean fresh dot update_psql dbinit dbclear dbupdate

all: build

GENERATOR=_build/src/codegen/hitscoregen.native

$(GENERATOR):
	ocamlbuild src/codegen/hitscoregen.native

LAYOUT_SOURCE=data/hitscore_layout

_build/hitscore_layout_digraph.dot: $(LAYOUT_SOURCE) $(GENERATOR)
	$(GENERATOR) digraph $(LAYOUT_SOURCE) $@

_doc/hitscore_layout_digraph.pdf: _build/hitscore_layout_digraph.dot _doc/
	dot -Tpdf $< -o$@

_doc/hitscore_layout_digraph.jpg: _build/hitscore_layout_digraph.dot _doc/
	dot -Tjpg $< -o$@

_doc/:
	mkdir _doc

dots: _doc/hitscore_layout_digraph.pdf _doc/hitscore_layout_digraph.jpg



dbclear:
	psql -qAtX -c "select 'DROP table ' || quote_ident(table_schema) \
          || '.' || quote_ident(table_name) \
          || ' CASCADE;' from information_schema.tables \
              where table_type = 'BASE TABLE' and \
              not table_schema ~ '^(information_schema|pg_.*)$$'" | psql -qAtX


PKG_VERSION=$(shell printf "`cat setup.data`\necho \$$pkg_version\n" | sed 's/ = /=/' | sh)
BINDIR=$(shell printf "`cat setup.data`\necho \$$bindir\n" | sed 's/ = /=/' | sh)

install-version: _build/src/app/hitscore setup.data
	cp $< $(BINDIR)/hitscore-$(PKG_VERSION)

build:
	ocaml setup.ml -build

install:
	ocaml setup.ml -reinstall

uninstall:
	ocaml setup.ml -uninstall

libdoc:
	ocaml setup.ml -doc

customdoc: dots

doc: libdoc customdoc
	mkdir -p _doc/lib
	cp hitscoredoc.docdir/* _doc/lib/

cleandoc:
	rm -fr _doc/

clean:
	ocaml setup.ml -clean


# clean everything and uninstall
fresh: clean uninstall

# clean setup files, rebuilding may require additional tools
distclean: clean
	ocaml setup.ml -distclean

