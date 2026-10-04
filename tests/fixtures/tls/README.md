Test-only TLS material for the client TLS tests in `src/client/mod.rs`.
Nothing here protects anything; the leaf key is public on purpose.

- `ca.pem`: test CA the tests trust.
- `leaf.pem` / `leaf.key`: server certificate signed by `ca.pem`,
  subjectAltName `DNS:localhost` only (so connecting by IP is a name mismatch).
- `other-ca.pem`: an unrelated CA, for the untrusted-issuer case.

ECDSA P-256, valid for 100 years. The CA private keys were discarded, so to
change the leaf, regenerate all of it with the `openssl` CLI.
