
Build and Install
=================

Fresh Build Environment
-----------------------

Get `vm4centos6.5-v1.0.tar.xz` form [vm4nerds.com](https://www.vm4nerds.com/).

    tar xfJ vm4centos6.5-v1.0.tar.xz

Edit `vm4nerds_centos_6.5-x86_64.sh` to set `MEM=1024` (needed for the
compilation of a library).

    bash vm4nerds_centos_6.5-x86_64.sh

The password is `vm4nerds`

    ssh -p 2222 root@localhost

Get the EPEL repositories for yum:

    wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    rpm -Uvh epel-release-6*.rpm

Install the 'C'

    yum -y install libev-devel.x86_64 sqlite-devel.x86_64 pam-devel.x86_64 git pcre-devel openssl-devel

Build Hitscore & Hitscoreweb
----------------------------

Get a decently recent OCaml compiler:

    mkdir ocaml_work
    cd ocaml_work
    wget -O ocaml.tgz http://caml.inria.fr/pub/distrib/ocaml-4.01/ocaml-4.01.0.tar.gz
    tar xvfs ocaml.tgz
    cd ocaml-4.01.0
    ./configure --prefix $HOME/usr/
    make world.opt > build.log 2>&1
    make install
    export PATH=$HOME/usr/bin:$PATH
    cd ..

Compile & Install the Opam package manager:

    wget -O opam.tgz https://github.com/ocaml/opam/releases/download/1.1.1/opam-full-1.1.1.tar.gz
    tar xvfz opam.tgz
    cd opam-full*
    ./configure --prefix $HOME/usr/
    make
    make install
    cd ..

    export OPAMROOT=$HOME/ocaml_work/opamroot
    opam init
    eval `opam config env`

The following lines can be put in `ocaml_opam.env` for reuse:

    echo 'export PATH=$HOME/usr/bin:$PATH
    export OPAMROOT=$HOME/ocaml_work/opamroot
    eval `opam config env`' > ocaml_opam.env

The code repositories are:

    NYU_OPAM_REPO=git@github.com:smondet/nyu-opam-repo

The user doing this must have their SSH keys registered on Github, and
`ssh-agent` running (and there may a `yes` or two to type during the
installations)..

    opam remote add nyu $NYU_OPAM_REPO
    opam update
    opam install --yes hitscore jsonm eliom re

