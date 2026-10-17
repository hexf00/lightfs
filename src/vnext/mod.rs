//! vnext: the long-term metadata & versioning core (S4.19).
//!
//! Authority model — everything durable descends from mutable REFS:
//!
//! ```text
//! refs(name) --CAS--> commits(id, parent, root_tree)   immutable
//!                        commits --> trees(id, entries)  immutable COW
//!                        trees   --> (file_id, version) --> file_versions  immutable
//!                        file_versions --> object_key --> immutable data object
//! ```
//!
//! - A commit is an immutable namespace state: `{parent, root_tree}`.
//! - Directory nodes are copy-on-write: an update creates new tree rows
//!   only along the changed path; untouched subtrees are shared, so
//! - branch creation copies a ref (zero data), and branch isolation is
//!   structural (each branch head folds its own chain).
//! - `file_id` is the STABLE file identity across commits and versions —
//!   the by-id protocol anchor survives; only directory nodes change id
//!   when they change.
//! - The mutable `refs` table is the ONLY mutable state; mainline
//!   commits advance it inside the same SQLite transaction that appends
//!   the commit, and explicit branch-head updates go through a
//!   compare-and-swap that refuses to silently overwrite.
//! - Data objects are registered in the `objects` ledger BEFORE the
//!   metadata transaction; reachability GC deletes exactly
//!   `ledger − reachable(all refs)`, so a crash can only orphan objects
//!   that GC can then collect — never lose committed ones.
//!
//! There is NO mutable projection of the current tree (the v0.1 `nodes`
//! table is gone): the ref chain is the single correctness authority.
//! A materialized head view may be added later as a REBUILDABLE cache.
//!
//! SQLite stays the single-server execution layer: INSERT-only rows for
//! everything immutable, one row-level CAS for refs. Old v0.1 databases
//! are refused at open (explicitly, never cleared).

pub mod core;
pub mod envelope;
pub mod gc;
pub mod recover;
pub mod store;

#[cfg(test)]
mod slice_tests;

pub use core::Core;
pub use store::Store;
