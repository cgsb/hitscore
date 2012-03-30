#!/bin/bash

# SSL Helper Utility
#
# (c) 2010 Christian Schneider <software(at)chschneider(dot)eu>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3 as
# published by the Free Software Foundation, not any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# WARNING: THIS IS ALPHA SOFTWARE AND MAY CONTAIN SERIOUS BUGS!

# #####################################################################
# configuration
# #####################################################################

## DN values
COUNTRY="DE"
PROVINCE="NO"
CITY="NOWHERE"
ORG="EXAMPLE.COM"
ORGUNIT="FOOBAR"
CN="CN"
EMAIL="root@example.com"

## where to store keys, certificates, and other OpenSSL related stuff
[[ -z "${CERTSDIR}" ]] && CERTSDIR="${HOME}/certs"

## configuration file (if does not exists, will automatically be generated)
[[ -z "${SSLCONF}" ]] && SSLCONF="${CERTSDIR}/openssl.cnf"

## RSA key size
KEYSIZE="4096"

## DH parameter size
DHSIZE="2048"

## validities (in days)
[[ -z "${VALIDDEFAULT}" ]] && VALIDDEFAULT="3650"
[[ -z "${VALIDCA}" ]]      && VALIDCA="${VALIDDEFAULT}"
[[ -z "${VALIDCLIENT}" ]]  && VALIDCLIENT="365"
[[ -z "${VALIDCRL}" ]]     && VALIDCRL="30"

# #####################################################################
# script code -- NO CHANGES BELOW THIS LINE!
# #####################################################################

## version of this tool
VERSION="20100728"

## exit if a command fails
set -e

## for OpenVPN etc.
export PATH="/sbin:/usr/sbin:/usr/local/sbin:${PATH}"

## export some variables
export CERTSDIR
export VALIDDEFAULT
export VALIDCRL
export KEYSIZE
export COUNTRY
export PROVINCE
export CITY
export ORG
export ORGUNIT
export CN
export EMAIL

exit_error () {
  echo "ERROR: ${1}" >&2
  exit 2
}

fn_openssl_conf () {
  cat << __EOF__
#
# OpenSSL configuration for ssl-util.
# Based on sample configuration shipped with OpenVPN.
#

RANDFILE               = \$ENV::CERTSDIR/.rand

openssl_conf           = openssl_init

[ openssl_init ]

oid_section            = new_oids
engines                = engine_section

[ new_oids ]

[ engine_section ]

[ ca ]
default_ca             = CA_default

[ CA_default ]

dir                    = \$ENV::CERTSDIR
certs                  = \$dir
crl_dir                = \$dir
database               = \$dir/index.txt
new_certs_dir          = \$dir
certificate            = \$dir/ca.crt
serial                 = \$dir/serial
crl                    = \$dir/crl.pem
private_key            = \$dir/ca.key
RANDFILE               = \$dir/.rand
x509_extensions        = usr_cert
default_days           = \$ENV::VALIDDEFAULT
default_crl_days       = \$ENV::VALIDCRL
# IMPORTANT: The next must no longer be md5, if used with
# Debian's OpenLDAP package being compiled against libgnutls.
default_md             = sha1
preserve               = no
policy                 = policy_match

[ policy_match ]

countryName            = match
stateOrProvinceName    = match
organizationName       = match
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

[ policy_anything ]

countryName            = optional
stateOrProvinceName    = optional
localityName           = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

[ req ]

default_bits           = \$ENV::KEYSIZE
default_keyfile        = privkey.pem
distinguished_name     = req_distinguished_name
attributes             = req_attributes
x509_extensions        = v3_ca
string_mask            = nombstr

[ req_distinguished_name ]

countryName            = Country Name (2 letter code)
countryName_default    = \$ENV::COUNTRY
countryName_min        = 2
countryName_max        = 2

stateOrProvinceName    = State or Province Name (full name)
stateOrProvinceName_default = \$ENV::PROVINCE

localityName           = Locality Name (eg, city)
localityName_default   = \$ENV::CITY

0.organizationName     = Organization Name (eg, company)
0.organizationName_default = \$ENV::ORG

organizationalUnitName = Organizational Unit Name (eg, section)
organizationalUnitName_default = \$ENV::ORGUNIT

commonName             = Common Name (eg, your name or your server\'s hostname)
commonName_default     = \$ENV::CN
commonName_max         = 64

emailAddress           = Email Address
emailAddress_default   = \$ENV::EMAIL
emailAddress_max       = 40

[ req_attributes ]

challengePassword      = A challenge password
challengePassword_min  = 4
challengePassword_max  = 20
unstructuredName       = An optional company name

[ usr_cert ]

basicConstraints       = CA:FALSE
nsComment              = "OpenSSL Generated Certificate"
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer:always
extendedKeyUsage       = clientAuth
keyUsage               = digitalSignature

[ server ]

basicConstraints       = CA:FALSE
nsCertType             = server
nsComment              = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer:always
extendedKeyUsage       = serverAuth
keyUsage               = digitalSignature, keyEncipherment

[ v3_req ]

basicConstraints       = CA:FALSE
keyUsage               = nonRepudiation,digitalSignature,keyEncipherment

[ v3_ca ]

subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints       = CA:true

[ crl_ext ]

authorityKeyIdentifier = keyid:always,issuer:always
__EOF__
}

fn_help () {
  echo "Usage: ${0##*/} <command>"
  echo "   or: ${0##*/} <command> <common name>"
  echo
  echo "Supported \`<command>'s without \`<common name>':"
  echo "  ca          - generate a certificate authority"
  echo "  crl         - generate/update a CRL"
  echo "  dhparam     - generate DH parameter file"
  echo "  openvpn_key - generate OpenVPN key file"
  echo
  echo "Supported \`<command>'s requiring a \`<common name>':"
  echo "  server      - generate a server key and certificate"
  echo "  client      - generate a client key and certificate"
  echo "  revoke      - revoke a certificate (implies \`crl' and"
  echo "                \`verify_crl')"
  echo "  verify_crl  - verify a certificate against the CA considering"
  echo "                also the current CRL (will not be generated!)"
  echo "  show        - show certificate details"
  echo "  verify      - verify a certificate against the CA"
  echo "Note 1: \`<common name>' is typically the FQDN of the host."
  echo "Note 2: the last two commands also work for arbitrary cert files"
  echo
  echo "Directory and file names:"
  echo "  main directory:  \`${CERTSDIR}'"
  echo "  keys/certs:      \`<common name>.key/<common name>.crt'"
  echo "  CA file names:   \`ca.crt' and \`ca.key'"
  echo "  CRL file name:   \`crl.pem'"
  echo "  DH param file:   \`dh${DHSIZE}.pem'"
  echo "  OpenVPN key:     \`ta.key'"
  echo
  echo "version of ${0##*/}: \`${VERSION}'"
  echo "license: GNU GPLv3 -- USE ON YOUR OWN RISK."
}

fn_info () {
  echo
  echo "*****************************************************"
  echo "${1}"
  echo "*****************************************************"
  echo
}

fn_requires () {
  [[ -z "${1}" ]] && exit_error "Required argument not supplied."
  [[ -f "${CERTSDIR}/${1}.crt" ]] || \
    exit_error "No certificate for CN \`${1}' found."
  [[ -f "${CERTSDIR}/${1}.key" ]] || \
    exit_error "No key for CN \`${1}' found."
  return 0
}

fn_conflicts () {
  [[ -z "${1}" ]] && exit_error "Required argument not supplied."
  [[ -f "${CERTSDIR}/${1}.crt" ]] && \
    exit_error "Certificate for CN \`${1}' already exists."
  [[ -f "${CERTSDIR}/${1}.key" ]] && \
    exit_error "Key for CN \`${1}' already exists."
  return 0
}

fn_dir () {
  [[ -d "${CERTSDIR}" ]] || mkdir -m 700 "${CERTSDIR}"
  [[ -f "${CERTSDIR}/index.txt" ]] || touch "${CERTSDIR}/index.txt"
  [[ -f "${CERTSDIR}/serial" ]] || echo "01" > "${CERTSDIR}/serial"
  [[ -f "${SSLCONF}" ]] || fn_openssl_conf > "${SSLCONF}"
  #cd "${CERTSDIR}"
  return "${?}"
}

fn_ca () {
  fn_conflicts "ca"
  fn_info "Generating CA ..."
  CN="CA" openssl req -batch -config "${SSLCONF}" -nodes -new -x509 \
    -days "${VALIDCA}" -keyout "${CERTSDIR}/ca.key" -out "${CERTSDIR}/ca.crt"
  return "${?}"
}

fn_server () {
  fn_requires "ca"
  fn_conflicts "${1}"
  fn_info "Generating server key/certificate for \`${1}' ..."
  CN="${1}" openssl req -batch -config "${SSLCONF}" \
    -nodes -new -extensions server \
    -keyout "${CERTSDIR}/${1}.key" -out "${CERTSDIR}/${1}.csr" && \
    openssl ca -batch -config "${SSLCONF}" -extensions server \
    -out "${CERTSDIR}/${1}.crt" -in "${CERTSDIR}/${1}.csr"
  return "${?}"
}

fn_client () {
  fn_requires "ca"
  fn_conflicts "${1}"
  fn_info "Generating client key/certificate for \`${1}' ..."
  CN="${1}" openssl req -batch -config "${SSLCONF}" \
    -days "${VALIDCLIENT}" -new \
    -keyout "${CERTSDIR}/${1}.key" -out "${CERTSDIR}/${1}.csr" && \
    openssl ca -batch -config "${SSLCONF}" -days "${VALIDCLIENT}" \
    -out "${CERTSDIR}/${1}.crt" -in "${CERTSDIR}/${1}.csr"
  return "${?}"
}

fn_revoke () {
  fn_requires "ca"
  fn_requires "${1}"
  fn_info "Revoking \`${1}' ..."
  openssl ca -config "${SSLCONF}" -revoke "${CERTSDIR}/${1}.crt"
  return "${?}"
}

fn_crl () {
  fn_requires "ca"
  fn_info "Generating CRL ..."
  openssl ca -config "${SSLCONF}" -gencrl -out "${CERTSDIR}/crl.pem"
  return "${?}"
}

fn_verify_crl () {
  fn_requires "ca"
  fn_requires "${1}"
  [[ -f "${CERTSDIR}/crl.pem" ]] || exit_error "No CRL found."
  fn_info "Verifying \`${1}' against CA and CRL ..."
  cat "${CERTSDIR}/ca.crt" "${CERTSDIR}/crl.pem" > \
    "${CERTSDIR}/.revoke-test.pem" && \
    openssl verify -CAfile "${CERTSDIR}/.revoke-test.pem" \
    -crl_check "${CERTSDIR}/${1}.crt"
  return "${?}"
}

fn_show () {
  CERT="${1}"
  [[ -f "${1}" ]] || CERT="${CERTSDIR}/${1}.crt"
  [[ -f "${CERT}" ]] || exit_error "Certificate not found."
  fn_info "Showing certificate \`${1}' ..."
  openssl x509 -in "${CERT}" -noout -text
  return "${?}"
}

fn_verify () {
  fn_requires "ca"
  CERT="${1}"
  [[ -f "${1}" ]] || CERT="${CERTSDIR}/${1}.crt"
  [[ -f "${CERT}" ]] || exit_error "Certificate not found."
  fn_info "Verifying \`${1}' against CA (*not* CRL!) ..."
  openssl verify -CAfile "${CERTSDIR}/ca.crt" "${CERT}"
  return "${?}"
}

fn_dhparam () {
  [[ -f "${CERTSDIR}/dh${DHSIZE}.pem" ]] && \
    exit_error "DH parameter file already exists."
  fn_info "Generating DH parameters ..."
  openssl dhparam -out "${CERTSDIR}/dh${DHSIZE}.pem" "${DHSIZE}"
  return "${?}"
}

fn_openvpn_key () {
  [[ -f "${CERTSDIR}/ta.key" ]] && \
    exit_error "OpenVPN key file already exists."
  fn_info "Generating OpenVPN key ..."
  openvpn --genkey --secret "${CERTSDIR}/ta.key"
  return "${?}"
}

case "${1}" in
  help|--help|-h|version|--version|-v)
    fn_help
    ;;
  ca)
    fn_dir
    fn_ca
    ;;
  server)
    fn_dir
    fn_server "${2}"
    ;;
  client)
    fn_dir
    fn_client "${2}"
    ;;
  crl)
    fn_dir
    fn_crl
    echo
    echo "*****************************************************"
    echo "* NOTE: YOU HAVE TO DISTRIBUTE THE UPDATED CRL NOW. *"
    echo "*****************************************************"
    ;;
  verify_crl)
    fn_dir
    fn_verify_crl "${2}"
    ;;
  revoke)
    fn_dir
    fn_revoke "${2}"
    fn_crl
    fn_verify_crl "${2}"
    echo
    echo "*****************************************************"
    echo "* NOTE: YOU HAVE TO DISTRIBUTE THE UPDATED CRL NOW. *"
    echo "*****************************************************"
    ;;
  show)
    fn_show "${2}"
    ;;
  verify)
    fn_verify "${2}"
    ;;
  dhparam)
    fn_dir
    fn_dhparam
    ;;
  openvpn_key)
    fn_dir
    fn_openvpn_key
    ;;
  *)
    exit_error "Unsupported command."
    ;;
esac
