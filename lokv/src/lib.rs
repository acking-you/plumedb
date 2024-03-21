#![deny(missing_docs, rustdoc::broken_intra_doc_links)]
#![cfg_attr(all(doc, CHANNEL_NIGHTLY), feature(doc_auto_cfg))]

//! # plumekv
//!
//! `plumekv` is a simple KV storage implementation
pub mod common;
pub mod storage;
pub mod table;
