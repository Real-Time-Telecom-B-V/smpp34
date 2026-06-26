# Versioning

`smpp34` follows [Semantic Versioning 2.0.0](https://semver.org/).

Unlike an application, **smpp34 is a library — its public Rust API _is_ the
contract.** Everything reachable as `pub` from the crate root (the codec types,
`SmppClient` / `SmppServer`, the `SmppClientListener` / `SmppServerListener`
traits, the PDU structs and their methods, and the TLV API) is covered.

## The git tag is the source of truth

`Cargo.toml`'s `version` is set to match the release tag, and the release
workflow's `verify-version` job **refuses to publish** if they disagree. To
release, bump `version`, commit, tag `vX.Y.Z`, and push the tag — the tag push
publishes the crate (and the GitHub Release) at `X.Y.Z`.

## The rule

**MAJOR (`X.0.0`)** — breaks the public API:

- Remove / rename / change the signature of a `pub` item.
- Change documented behavior in a way that breaks existing callers.
- Removals happen only **one minor after** a deprecation.

**MINOR (`x.Y.0`)** — backward-compatible additions:

- New `pub` items (functions, methods, PDU support, listener hooks with
  defaults, config knobs).
- **Deprecations** — mark deprecated, keep it working (removal is the next major).
- An MSRV bump (called out in the changelog).

**PATCH (`x.y.Z`)** — backward-compatible fixes:

- Bug fixes, performance improvements, behavior-neutral dependency bumps.
- **SMPP 3.4 conformance corrections** — *even when they change observable wire
  behavior.* The contract is "spec-compliant", so a correction toward the
  specification is a fix, not a break. **Document it loudly in the changelog.**

## Pre-1.0

While the crate is `0.x`, Cargo treats a **minor** bump as the breaking
increment. Breaking API changes therefore ship as `0.(y+1).0` and additive
changes as `0.y.(z+1)`. Pin a minor version (`smpp34 = "0.1"`) if you need
stability across `0.x` releases. A `1.0.0` release will lock the API surface to
the rules above.

## Pre-releases

`X.Y.Z-rc.N` for validation before a stable tag. The crates.io "newest" pointer
advances only on stable releases.
