//! wasl-transfer — the transfer tool (WX): pure scp -3 / rsync argv builders for
//! direct host-A->host-B (and A->many) transfers, plus a `Runner` trait so the
//! actual process exec lives behind a mockable seam. See `docs/specs/transfer.md`.
