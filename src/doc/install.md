
Build and Install
=================

Fresh Build Environment
-----------------------

> *This is useless when using a puppet-confiured build-VM*


Get `vm4centos6.5-v1.0.tar.xz` form [vm4nerds.com](https://www.vm4nerds.com/).

    tar xfJ vm4centos6.5-v1.0.tar.xz

Edit `vm4nerds_centos_6.5-x86_64.sh` to set `MEM=2048` (needed for the
compilation of some generated code libraries).

    bash vm4nerds_centos_6.5-x86_64.sh

The password is `vm4nerds`

    ssh -p 2222 root@localhost

Get the EPEL repositories for yum:

    wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    rpm -Uvh epel-release-6*.rpm

Install the 'C'

    yum -y install libev-devel.x86_64 sqlite-devel.x86_64 pam-devel.x86_64 git pcre-devel openssl-devel

All the steps in the following sections will be fine but as of *Wed, 05 Mar
2014 15:36:19 -0500*, deployment fails with:

    error: Failed dependencies:
        libcrypto.so.10(libcrypto.so.10)(64bit) is needed by hitscoreweb-1.6-1.x86_64
        libssl.so.10(libssl.so.10)(64bit) is needed by hitscoreweb-1.6-1.x86_64


Get Ocaml and Opam
------------------

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

Build Hitscore & Hitscoreweb
----------------------------

Let's initialize an Opam root directory:

    export OPAMROOT=$HOME/ocaml_work/opamroot
    opam init
    eval `opam config env`

The following lines can be put in `ocaml_opam.env` for reuse:

    echo 'export PATH=$HOME/usr/bin:$PATH
    export OPAMROOT=$HOME/ocaml_work/opamroot
    eval `opam config env`' > ocaml_opam.env

The code repositories are:

    NYU_OPAM_REPO=git@github.com:smondet/nyu-opam-repo
    HITSCOREWEB_REPO=git@github.com:smondet/hitscoreweb

The user doing this must have their SSH keys registered on Github, and
`ssh-agent` running (and there may a `yes` or two to type during the
installations because of SSH “new hosts”).

    opam remote add nyu $NYU_OPAM_REPO
    opam update
    opam install --yes hitscore jsonm eliom re

This will download and install Hitscore, all its dependencies, and the
additional packages needed for Hiscoreweb.
So, then it is Hitscoreweb's turn

    git clone $HITSCOREWEB_REPO
    cd hitscoreweb/
    make static && make static

(yes, twice because with some versions of make, one step is missed sometimes …)

There should be two executables: `hsw_manager` and `hitscoreserver`.

The *master* configuration file is on the HPC cluster in the account `gencore`:
`~/.config/hitscore/config.sexp`.

Once we have the `config.sexp` file, let's make the RPM for WSO1 with the
production database:

    ./hsw_manager config.sexp:wso1production rpm \
       -ssl gencore.bio.nyu.edu.crt  gencore.bio.nyu.edu.nopass.key
       -ssl-dir  /etc/ssl/gencore.bio.nyu.edu/ -pam login

This creates the file
`/tmp/hitscorerpmbuild/RPMS/x86_64/hitscoreweb-1.6-1.x86_64.rpm` which
can be pushed to `WSO1`.

    scp /tmp/hitscorerpmbuild/RPMS/x86_64/hitscoreweb-1.6-1.x86_64.rpm <user>@wso1.bio.nyu.edu:

Then on `WSO1` **as root**.

First, one can check if any user is going to be annoyed by the restart there
<https://gencore.bio.nyu.edu/log>. It's also nice to let the website run in
read-only mode for while:

    echo 'maintenance:on' > /var/hitscoreweb/ocsigen_command

Then:

    service hitscoreweb stop
    killall hitscoreserver
    rpm -e hitscoreweb
    rpm -i hitscoreweb-1.6-1.x86_64.rpm
    service hitscoreweb start


