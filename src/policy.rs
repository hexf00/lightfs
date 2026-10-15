//! Path policy: pure configuration parsing, validation, and matching.
//!
//! This module implements ONLY the deterministic core of the path-policy
//! design (declarative path policy & storage routing, Phase O):
//!
//! - input: an already-deserialized [`PolicySection`] (the caller reads the
//!   TOML file; this module never touches the filesystem),
//! - output: a validated [`ResolvedPolicy`] and per-path [`PolicyView`]s,
//! - NO network, NO disk IO, NO mounting, NO cleanup — anything that would
//!   make a test need a machine belongs to later phases (RoutingFs & co).
//!
//! Validation is total and loud: every rejected build names its reason and
//! there is NO code path that silently falls back to the default policy.
//! The default policy is not a fallback at all — it is the explicit default
//! rule (prefix "/", s3, no cache, strict), exactly the Phase-M behavior.

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Raw configuration surface (what the TOML deserializes into)
// ---------------------------------------------------------------------------

/// `[policy]` section. Absent from the config = `PolicySection::default()`
/// = today's behavior.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct PolicySection {
    /// Mount-wide budget ceilings. Optional; per-rule budgets that reference
    /// a missing global ceiling are a build error.
    #[serde(default)]
    pub limits: Option<PolicyLimits>,
    /// `[[policy.rule]]` array. Declaration order is irrelevant: matching is
    /// longest-prefix, and validation is order-independent.
    #[serde(default)]
    pub rule: Vec<RawRule>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct PolicyLimits {
    pub memory_budget: Option<String>,
    pub disk_budget: Option<String>,
}

/// One `[[policy.rule]]` as written. Strings (not enums) on purpose: each
/// rejected value gets a specific message, not a generic serde error.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct RawRule {
    pub prefix: String,
    /// "s3" | "client-local"
    pub storage: String,
    /// client-local only: where the authoritative bytes live.
    pub root: Option<String>,
    /// "none" | "memory" | "disk" | "memory+disk" (s3 rules only)
    pub cache: Option<String>,
    /// required iff cache includes disk (s3 rules only)
    pub cache_root: Option<String>,
    pub memory_budget: Option<String>,
    pub disk_budget: Option<String>,
    /// clean-cache eviction: "persistent" | "task" | "capacity"
    /// (s3 rules only; default "capacity")
    pub lifecycle: Option<String>,
    /// authoritative-data lifecycle: "persistent" | "task"
    /// (client-local rules only; default "persistent")
    pub authority_life: Option<String>,
    /// "strict" | "write-back" ("write-back" is reserved and rejected in v1)
    pub commit: Option<String>,
    /// opt-in to shadowing a remote directory (enforcement is a mount-time
    /// remote lookup — NOT this module's business; parse-only surface here)
    pub allow_shadow: Option<bool>,
}

// ---------------------------------------------------------------------------
// Resolved model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    Remote,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheKind {
    None,
    Memory,
    Disk,
    MemoryAndDisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    StrictSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEviction {
    Persistent,
    TaskScoped,
    CapacityLru,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityLife {
    Persistent,
    TaskScoped,
}

/// The per-path decision. `cache_eviction` is None exactly when
/// `read_cache` is None (nothing to evict).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyView {
    /// The matched rule's prefix ("/" for the default rule). Diagnostics.
    pub rule_prefix: String,
    pub authority: Authority,
    pub read_cache: CacheKind,
    pub commit: CommitMode,
    pub authority_life: AuthorityLife,
    pub cache_eviction: Option<CacheEviction>,
}

#[derive(Debug, Clone)]
struct Rule {
    /// Normalized prefix ("/a/b"). The default rule is "/".
    prefix: String,
    view: PolicyView,
    /// Parsed budgets (bytes), kept for the global-overcommit check.
    memory_budget: Option<u64>,
    disk_budget: Option<u64>,
}

/// Mount-wide ceilings (parsed bytes), present only if configured.
#[derive(Debug, Clone, Copy, Default)]
pub struct GlobalLimits {
    pub memory_budget: Option<u64>,
    pub disk_budget: Option<u64>,
}

/// A fully validated policy set, ready for O(1)-per-call matching
/// (linear over a handful of rules, longest prefix first).
#[derive(Debug, Clone)]
pub struct ResolvedPolicy {
    /// Sorted longest-prefix first; ALWAYS ends with the default rule "/"
    /// (explicitly configured "/" rules replace the implicit one).
    rules: Vec<Rule>,
    pub limits: GlobalLimits,
}

// ---------------------------------------------------------------------------
// Errors — every rejection names its reason; no silent fallbacks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// Path (query or rule prefix) is not a valid absolute mount-relative
    /// path.
    InvalidPath(String),
    /// A rule field names a value this version does not know.
    UnknownValue { field: &'static str, value: String },
    /// Two rules claim the same prefix — ambiguous by definition.
    DuplicatePrefix(String),
    /// `write-back` is a reserved variant (s3: needs its own design review;
    /// client-local: meaningless). Rejected, never downgraded.
    CommitReserved { storage: &'static str },
    /// A Local rule cannot have a cache (no authoritative version signal —
    /// path-policy design matrix).
    LocalWithCache { prefix: String },
    /// Cache-eviction config on a rule that has no cache.
    EvictionWithoutCache { prefix: String },
    /// `authority_life` on an s3 rule (only Local authority can be scoped).
    RemoteWithAuthorityLife { prefix: String },
    /// `root` on an s3 rule (remote rules have no local root).
    RootOnRemote { prefix: String },
    /// A Local rule without `root`.
    MissingRoot { prefix: String },
    /// A budget field present where no cache can use it.
    BudgetWithoutCache { prefix: String },
    /// A cache kind that needs a budget is missing it.
    MissingBudget { prefix: String, field: &'static str },
    /// Budget string malformed or non-positive.
    BadBudget { field: String, value: String },
    /// Σ per-rule budgets exceed the mount-wide ceiling.
    BudgetOvercommit { kind: &'static str },
    /// A per-rule budget references a global ceiling that is not configured.
    MissingGlobalLimit { kind: &'static str },
    /// Local root malformed or unsafe (relative, ".." segment, "/" or "~",
    /// or duplicated across rules).
    UnsafeRoot { prefix: String, reason: String },
    /// A cache kind whose implementation (CacheStore) does not exist yet.
    /// Rejected, never silently ignored (P-R2): a config that asks for a
    /// cache must not mount looking like the cache is active.
    CacheUnavailable { prefix: String, kind: &'static str },
    /// `authority_life = "task"` (TaskScoped cleanup) has no implementation
    /// yet (P-R2).
    TaskScopedUnavailable { prefix: String },
    /// A cache eviction (`lifecycle`) value whose semantics are not
    /// implemented for the single-tier memory read cache (Q1): for a purely
    /// in-memory cache there is no distinguishable, testable behavior
    /// between "persistent" (evict only under pressure) and the default
    /// capacity LRU — both can only evict LRU-first when the hard budget is
    /// hit. Rejected rather than silently running the LRU anyway.
    EvictionUnavailable { prefix: String, kind: &'static str },
    /// More than one rule enables the memory read cache. The first cache
    /// implementation carries exactly one (Q1): a second cached rule must
    /// refuse the mount, never silently share or skip the cache.
    TooManyCachedRules { prefixes: Vec<String> },
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::InvalidPath(p) => {
                write!(
                    f,
                    "invalid policy path {p:?}: must be absolute and mount-relative"
                )
            }
            PolicyError::UnknownValue { field, value } => {
                write!(f, "policy rule field {field:?} has unknown value {value:?}")
            }
            PolicyError::DuplicatePrefix(p) => {
                write!(
                    f,
                    "duplicate policy prefix {p:?}: two rules claim the same prefix"
                )
            }
            PolicyError::CommitReserved { storage } => write!(
                f,
                "commit = \"write-back\" is reserved and rejected in this version ({storage} rule); \
                 the write-back design is a separate, unmerged proposal"
            ),
            PolicyError::LocalWithCache { prefix } => write!(
                f,
                "rule {prefix:?}: client-local storage cannot have a read cache — local data \
                 carries no authoritative version, so invalidation would be a guess"
            ),
            PolicyError::EvictionWithoutCache { prefix } => write!(
                f,
                "rule {prefix:?}: lifecycle config requires a cache; there is nothing to evict"
            ),
            PolicyError::RemoteWithAuthorityLife { prefix } => write!(
                f,
                "rule {prefix:?}: authority_life applies to client-local rules only — s3 \
                 authoritative data is never deleted by client policy"
            ),
            PolicyError::CacheUnavailable { prefix, kind } => write!(
                f,
                "rule {prefix:?}: cache {kind:?} has no implementation yet (CacheStore is \
                 unbuilt) — the mount is refused rather than running without the \
                 optimization it asked for"
            ),
            PolicyError::TaskScopedUnavailable { prefix } => write!(
                f,
                "rule {prefix:?}: authority_life = \"task\" (TaskScoped cleanup) has no \
                 implementation yet — the mount is refused rather than leaking local \
                 files past their promised scope"
            ),
            PolicyError::EvictionUnavailable { prefix, kind } => write!(
                f,
                "rule {prefix:?}: lifecycle = {kind:?} has no distinct implementation for the \
                 single-tier memory read cache (only the default capacity LRU exists; a purely \
                 in-memory cache can only ever evict LRU-first under budget pressure) — \
                 the mount is refused rather than running the default while claiming otherwise"
            ),
            PolicyError::TooManyCachedRules { prefixes } => write!(
                f,
                "policy: at most one memory-cached rule is supported in this version (got {}: \
                 {}) — set cache = \"none\" on the extra rules",
                prefixes.len(),
                prefixes.join(", ")
            ),
            PolicyError::RootOnRemote { prefix } => {
                write!(
                    f,
                    "rule {prefix:?}: root applies to client-local rules only"
                )
            }
            PolicyError::MissingRoot { prefix } => {
                write!(f, "rule {prefix:?}: client-local storage requires root")
            }
            PolicyError::BudgetWithoutCache { prefix } => write!(
                f,
                "rule {prefix:?}: budget/cache_root fields require a cache; cache is none"
            ),
            PolicyError::MissingBudget { prefix, field } => {
                write!(f, "rule {prefix:?}: cache kind requires {field} to be set")
            }
            PolicyError::BadBudget { field, value } => {
                write!(
                    f,
                    "policy {field}: invalid budget {value:?} (expected e.g. \"64MiB\")"
                )
            }
            PolicyError::BudgetOvercommit { kind } => write!(
                f,
                "policy: sum of per-rule {kind} budgets exceeds the mount-wide limit"
            ),
            PolicyError::MissingGlobalLimit { kind } => write!(
                f,
                "policy: a per-rule {kind} budget needs [policy.limits] {kind}_budget"
            ),
            PolicyError::UnsafeRoot { prefix, reason } => {
                write!(f, "rule {prefix:?}: unsafe local root: {reason}")
            }
        }
    }
}

impl std::error::Error for PolicyError {}

// ---------------------------------------------------------------------------
// Path normalization (shared by rule prefixes and queries)
// ---------------------------------------------------------------------------

/// Normalize an absolute mount-relative path:
/// - collapses repeated separators (`//work///x` → `/work/x`),
/// - drops `.` components,
/// - resolves `..` by popping; `..` at the root truncates to the root (the
///   same root-pinning contract as symlink resolution in core),
/// - a trailing separator is just an empty component (dropped).
///
/// Deliberately LENIENT (normalizes instead of rejecting everything) so that
/// rule prefixes and query paths canonicalize to the same string — but it
/// never changes WHAT a path means, and it never accepts a relative path.
pub fn normalize_path(path: &str) -> Result<String, PolicyError> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(PolicyError::InvalidPath(path.to_string()));
    }
    let mut stack: Vec<&str> = Vec::new();
    for comp in path.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                stack.pop(); // pop; popping an empty stack = stay at root
            }
            other => stack.push(other),
        }
    }
    if stack.is_empty() {
        return Ok("/".to_string());
    }
    Ok(format!("/{}", stack.join("/")))
}

/// True when `path` is `prefix` itself or anywhere inside it, comparing by
/// path COMPONENTS: `/work/cache` does not match `/work/cache-old`.
pub(crate) fn prefix_matches(prefix: &str, path: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    path == prefix || path.starts_with(&format!("{prefix}/"))
}

// ---------------------------------------------------------------------------
// Budget parsing (pure string -> bytes)
// ---------------------------------------------------------------------------

/// Accepts `<n>` (bytes) or `<n>KiB|MiB|GiB|TiB` (binary units only — the
/// config surface deliberately has no KB/MB so no one can misread 1000 vs
/// 1024). Zero and overflow are errors.
pub fn parse_budget(field: &str, value: &str) -> Result<u64, PolicyError> {
    let bad = || PolicyError::BadBudget {
        field: field.to_string(),
        value: value.to_string(),
    };
    let (num, unit) = value.trim().split_at(
        value
            .trim()
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(value.trim().len()),
    );
    let n: u64 = num.parse().map_err(|_| bad())?;
    if n == 0 {
        return Err(bad());
    }
    let mult: u64 = match unit {
        "" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err(bad()),
    };
    n.checked_mul(mult).ok_or_else(bad)
}

// ---------------------------------------------------------------------------
// Build: raw config -> validated ResolvedPolicy
// ---------------------------------------------------------------------------

impl ResolvedPolicy {
    /// Validate everything, resolve defaults, and freeze. The ONLY entry
    /// point from raw config; there is no way to obtain a `ResolvedPolicy`
    /// with unvalidated rules.
    pub fn build(section: &PolicySection) -> Result<ResolvedPolicy, PolicyError> {
        let limits = GlobalLimits {
            memory_budget: match &section.limits {
                Some(l) => match &l.memory_budget {
                    Some(v) => Some(parse_budget("limits.memory_budget", v)?),
                    None => None,
                },
                None => None,
            },
            disk_budget: match &section.limits {
                Some(l) => match &l.disk_budget {
                    Some(v) => Some(parse_budget("limits.disk_budget", v)?),
                    None => None,
                },
                None => None,
            },
        };

        let mut rules: Vec<Rule> = Vec::new();
        for raw in &section.rule {
            rules.push(Self::build_rule(raw)?);
        }

        // Duplicate prefixes are ambiguous regardless of declaration order.
        let mut seen: Vec<&str> = Vec::new();
        for r in &rules {
            if seen.contains(&r.prefix.as_str()) {
                return Err(PolicyError::DuplicatePrefix(r.prefix.clone()));
            }
            seen.push(&r.prefix);
        }

        // Two local rules sharing one root would make two policies write the
        // same authoritative tree — refuse instead of arbitrating.
        let mut roots: Vec<&String> = Vec::new();
        for raw in &section.rule {
            if raw.storage == "client-local" {
                if let Some(root) = &raw.root {
                    if roots.contains(&root) {
                        return Err(PolicyError::UnsafeRoot {
                            prefix: raw.prefix.clone(),
                            reason: "two rules share the same local root".into(),
                        });
                    }
                    roots.push(root);
                }
            }
        }

        // Σ per-rule budgets must fit the mount-wide ceilings. A rule budget
        // without its ceiling is an error (it would be unenforceable).
        let mem_sum: u64 = rules.iter().filter_map(|r| r.memory_budget).sum();
        let disk_sum: u64 = rules.iter().filter_map(|r| r.disk_budget).sum();
        if mem_sum > 0 {
            let Some(g) = limits.memory_budget else {
                return Err(PolicyError::MissingGlobalLimit { kind: "memory" });
            };
            if mem_sum > g {
                return Err(PolicyError::BudgetOvercommit { kind: "memory" });
            }
        }
        if disk_sum > 0 {
            let Some(g) = limits.disk_budget else {
                return Err(PolicyError::MissingGlobalLimit { kind: "disk" });
            };
            if disk_sum > g {
                return Err(PolicyError::BudgetOvercommit { kind: "disk" });
            }
        }

        // Availability gate (P-R2, narrowed by Q1): everything above
        // validates CONFIG SHAPE; a well-formed request for something that
        // does not exist must still be refused, never mounted as if it were
        // active. Placed AFTER the model validation so malformed cache
        // configs keep their specific errors; placed BEFORE matching so no
        // resolved view can ever carry an unimplemented feature.
        //
        // Q1 opens exactly one combination: `storage = "s3"` +
        // `cache = "memory"` + strict commit + the default capacity-LRU
        // eviction, on ONE rule per mount. Everything else keeps its named
        // refusal.
        let mut cached: Vec<&str> = Vec::new();
        for r in &rules {
            match r.view.read_cache {
                CacheKind::None => {}
                CacheKind::Memory => {
                    // The single-tier memory cache has exactly one eviction
                    // behavior: LRU under the hard budget. "persistent" has
                    // no distinguishable, testable meaning without a second
                    // (disk) tier to protect, and "task" (TaskScoped
                    // cleanup) does not exist — both refuse by name.
                    match r.view.cache_eviction {
                        Some(CacheEviction::CapacityLru) => cached.push(&r.prefix),
                        Some(CacheEviction::Persistent) => {
                            return Err(PolicyError::EvictionUnavailable {
                                prefix: r.prefix.clone(),
                                kind: "persistent",
                            });
                        }
                        Some(CacheEviction::TaskScoped) => {
                            return Err(PolicyError::EvictionUnavailable {
                                prefix: r.prefix.clone(),
                                kind: "task",
                            });
                        }
                        None => unreachable!("cache rules always resolve an eviction value"),
                    }
                }
                kind @ (CacheKind::Disk | CacheKind::MemoryAndDisk) => {
                    return Err(PolicyError::CacheUnavailable {
                        prefix: r.prefix.clone(),
                        kind: match kind {
                            CacheKind::Disk => "disk",
                            CacheKind::MemoryAndDisk => "memory+disk",
                            _ => unreachable!(),
                        },
                    });
                }
            }
            if r.view.authority_life == AuthorityLife::TaskScoped {
                return Err(PolicyError::TaskScopedUnavailable {
                    prefix: r.prefix.clone(),
                });
            }
        }
        if cached.len() > 1 {
            return Err(PolicyError::TooManyCachedRules {
                prefixes: cached.into_iter().map(str::to_string).collect(),
            });
        }

        // Longest prefix first; equal lengths are unambiguous (distinct
        // prefixes never cross-match) so their relative order is irrelevant.
        rules.sort_by(|a, b| b.prefix.len().cmp(&a.prefix.len()));

        // The default rule is an explicit part of the model: absent a
        // user-written "/" rule, today's behavior IS the default policy.
        if !rules.iter().any(|r| r.prefix == "/") {
            rules.push(Rule {
                prefix: "/".to_string(),
                view: PolicyView {
                    rule_prefix: "/".to_string(),
                    authority: Authority::Remote,
                    read_cache: CacheKind::None,
                    commit: CommitMode::StrictSync,
                    authority_life: AuthorityLife::Persistent,
                    cache_eviction: None,
                },
                memory_budget: None,
                disk_budget: None,
            });
        }

        Ok(ResolvedPolicy { rules, limits })
    }

    fn build_rule(raw: &RawRule) -> Result<Rule, PolicyError> {
        let prefix = normalize_path(&raw.prefix)?;

        let storage = match raw.storage.as_str() {
            "s3" => Authority::Remote,
            "client-local" => Authority::Local,
            other => {
                return Err(PolicyError::UnknownValue {
                    field: "storage",
                    value: other.to_string(),
                });
            }
        };

        // Commit: only strict exists; write-back is reserved everywhere.
        match raw.commit.as_deref() {
            None | Some("strict") => {}
            Some("write-back") => {
                return Err(PolicyError::CommitReserved {
                    storage: if storage == Authority::Local {
                        "client-local"
                    } else {
                        "s3"
                    },
                });
            }
            Some(other) => {
                return Err(PolicyError::UnknownValue {
                    field: "commit",
                    value: other.to_string(),
                });
            }
        }

        // Cross-field shape depends on storage first.
        match storage {
            Authority::Local => {
                if raw.cache.is_some() || raw.cache_root.is_some() {
                    return Err(PolicyError::LocalWithCache { prefix });
                }
                if raw.memory_budget.is_some() || raw.disk_budget.is_some() {
                    return Err(PolicyError::BudgetWithoutCache { prefix });
                }
                if raw.lifecycle.is_some() {
                    // cache eviction has no meaning without a cache; the
                    // local knob is `authority_life`
                    return Err(PolicyError::EvictionWithoutCache { prefix });
                }
            }
            Authority::Remote => {
                if raw.root.is_some() {
                    return Err(PolicyError::RootOnRemote { prefix });
                }
                if raw.authority_life.is_some() {
                    return Err(PolicyError::RemoteWithAuthorityLife { prefix });
                }
            }
        }

        let (read_cache, cache_eviction, memory_budget, disk_budget) = match storage {
            Authority::Local => {
                let root = raw.root.as_deref().ok_or(PolicyError::MissingRoot {
                    prefix: prefix.clone(),
                })?;
                validate_local_root(&prefix, root)?;
                (CacheKind::None, None, None, None)
            }
            Authority::Remote => {
                let cache = match raw.cache.as_deref() {
                    None | Some("none") => CacheKind::None,
                    Some("memory") => CacheKind::Memory,
                    Some("disk") => CacheKind::Disk,
                    Some("memory+disk") => CacheKind::MemoryAndDisk,
                    Some(other) => {
                        return Err(PolicyError::UnknownValue {
                            field: "cache",
                            value: other.to_string(),
                        });
                    }
                };
                if cache == CacheKind::None {
                    // budgets/cache_root/lifecycle without a cache are a
                    // config mistake, never a silent ignore
                    if raw.cache_root.is_some()
                        || raw.memory_budget.is_some()
                        || raw.disk_budget.is_some()
                        || raw.lifecycle.is_some()
                    {
                        return Err(PolicyError::BudgetWithoutCache { prefix });
                    }
                    (CacheKind::None, None, None, None)
                } else {
                    let mem = if matches!(cache, CacheKind::Memory | CacheKind::MemoryAndDisk) {
                        let v = raw
                            .memory_budget
                            .as_deref()
                            .ok_or(PolicyError::MissingBudget {
                                prefix: prefix.clone(),
                                field: "memory_budget",
                            })?;
                        Some(parse_budget("memory_budget", v)?)
                    } else {
                        if raw.memory_budget.is_some() {
                            return Err(PolicyError::BudgetWithoutCache { prefix });
                        }
                        None
                    };
                    let (disk, _) = if matches!(cache, CacheKind::Disk | CacheKind::MemoryAndDisk) {
                        let v = raw
                            .disk_budget
                            .as_deref()
                            .ok_or(PolicyError::MissingBudget {
                                prefix: prefix.clone(),
                                field: "disk_budget",
                            })?;
                        let parsed = parse_budget("disk_budget", v)?;
                        let cr = raw
                            .cache_root
                            .as_deref()
                            .ok_or(PolicyError::MissingBudget {
                                prefix: prefix.clone(),
                                field: "cache_root",
                            })?;
                        if cr.is_empty() {
                            return Err(PolicyError::UnsafeRoot {
                                prefix,
                                reason: "cache_root is empty".into(),
                            });
                        }
                        (Some(parsed), ())
                    } else {
                        if raw.disk_budget.is_some() || raw.cache_root.is_some() {
                            return Err(PolicyError::BudgetWithoutCache { prefix });
                        }
                        (None, ())
                    };
                    let eviction = match raw.lifecycle.as_deref() {
                        None | Some("capacity") => CacheEviction::CapacityLru,
                        Some("persistent") => CacheEviction::Persistent,
                        Some("task") => CacheEviction::TaskScoped,
                        Some(other) => {
                            return Err(PolicyError::UnknownValue {
                                field: "lifecycle",
                                value: other.to_string(),
                            });
                        }
                    };
                    (cache, Some(eviction), mem, disk)
                }
            }
        };

        let authority_life = match storage {
            Authority::Local => match raw.authority_life.as_deref() {
                None | Some("persistent") => AuthorityLife::Persistent,
                Some("task") => AuthorityLife::TaskScoped,
                Some(other) => {
                    return Err(PolicyError::UnknownValue {
                        field: "authority_life",
                        value: other.to_string(),
                    });
                }
            },
            Authority::Remote => AuthorityLife::Persistent,
        };

        let view = PolicyView {
            rule_prefix: prefix.clone(),
            authority: storage,
            read_cache,
            commit: CommitMode::StrictSync,
            authority_life,
            cache_eviction,
        };
        Ok(Rule {
            prefix,
            view,
            memory_budget,
            disk_budget,
        })
    }

    /// Iterate (prefix, view) pairs, longest prefix first. The router uses
    /// this to find which resolved rules are client-local (and must own a
    /// backend) without re-implementing the matching.
    pub fn rules(&self) -> impl Iterator<Item = (&str, &PolicyView)> {
        self.rules.iter().map(|r| (r.prefix.as_str(), &r.view))
    }

    /// The single memory-cached rule as `(prefix, budget bytes)`, or None.
    /// The build gate guarantees at most one cached rule and a mandatory
    /// positive budget for it, so this is total — no silent default.
    pub fn memory_cache(&self) -> Option<(&str, u64)> {
        self.rules.iter().find_map(|r| {
            if r.view.read_cache == CacheKind::Memory {
                Some((
                    r.prefix.as_str(),
                    r.memory_budget
                        .expect("memory cache rule has a budget (build enforces)"),
                ))
            } else {
                None
            }
        })
    }

    /// Resolve `path` to its PolicyView. Unknown → the default rule (which
    /// is always present, so matching never "fails into" a fallback — the
    /// default IS a rule).
    pub fn resolve(&self, path: &str) -> Result<PolicyView, PolicyError> {
        let path = normalize_path(path)?;
        for r in &self.rules {
            if prefix_matches(&r.prefix, &path) {
                return Ok(r.view.clone());
            }
        }
        unreachable!("the default rule \"/\" matches every normalized path");
    }
}

/// Local-root shape checks (pure string work; existence/permissions are
/// runtime business for the mount phase):
/// - absolute or `~`-prefixed (the expansion itself happens at mount),
/// - no `..` anywhere (refused, not resolved: a root that escapes by
///   traversal is a config smell even when it resolves to somewhere safe),
/// - never "/" or "~" itself (handing a whole filesystem/home to a policy),
/// - repeated separators and trailing separators collapse.
fn validate_local_root(prefix: &str, root: &str) -> Result<(), PolicyError> {
    let unsafe_root = |reason: &str| PolicyError::UnsafeRoot {
        prefix: prefix.to_string(),
        reason: reason.to_string(),
    };
    if root.is_empty() {
        return Err(unsafe_root("root is empty"));
    }
    let tilde = root.starts_with("~/");
    // Platform-correct absoluteness: on Windows an absolute root is
    // drive-prefixed ("C:\\data"), which a '/'-string check would wrongly
    // refuse — every local rule would be unusable there.
    if !tilde && !std::path::Path::new(root).is_absolute() {
        return Err(unsafe_root("root must be absolute or start with \"~/\""));
    }
    if root == "/" || root == "~" || root == "~/" {
        return Err(unsafe_root(
            "root must name a subdirectory, not a whole filesystem/home",
        ));
    }
    // Namespace roots on any platform: "/" and Windows drive/UNC roots
    // ("C:\\", "\\\\srv\\share") have no parent — refusing them is the
    // same whole-filesystem handout refusal as above.
    if !tilde && std::path::Path::new(root).parent().is_none() {
        return Err(unsafe_root(
            "root must name a subdirectory, not a whole filesystem/home",
        ));
    }
    for comp in root.split(['/', '\\']) {
        if comp == ".." {
            return Err(unsafe_root("root contains \"..\" — refused, not resolved"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: the Phase-O checklist (O2), host-side, no machine needed
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(prefix: &str, storage: &str) -> RawRule {
        RawRule {
            prefix: prefix.to_string(),
            storage: storage.to_string(),
            root: None,
            cache: None,
            cache_root: None,
            memory_budget: None,
            disk_budget: None,
            lifecycle: None,
            authority_life: None,
            commit: None,
            allow_shadow: None,
        }
    }

    fn section(rules: Vec<RawRule>) -> PolicySection {
        PolicySection {
            limits: None,
            rule: rules,
        }
    }

    fn build(rules: Vec<RawRule>) -> ResolvedPolicy {
        ResolvedPolicy::build(&section(rules)).expect("build")
    }

    fn local(prefix: &str, root: &str) -> RawRule {
        let mut r = rule(prefix, "client-local");
        r.root = Some(root.to_string());
        r
    }

    fn cached_s3(prefix: &str, cache: &str) -> RawRule {
        let mut r = rule(prefix, "s3");
        r.cache = Some(cache.to_string());
        r
    }

    // -- O2.1: no configuration == today's default behavior ----------------

    #[test]
    fn empty_section_is_the_phase_m_default() {
        let p = build(vec![]);
        for path in ["/", "/x", "/a/b/c.txt"] {
            let v = p.resolve(path).expect("resolve");
            assert_eq!(v.rule_prefix, "/");
            assert_eq!(v.authority, Authority::Remote);
            assert_eq!(v.read_cache, CacheKind::None);
            assert_eq!(v.commit, CommitMode::StrictSync);
            assert_eq!(v.cache_eviction, None);
        }
    }

    // -- O2.2: longest prefix wins ------------------------------------------

    #[test]
    fn longest_prefix_wins() {
        let p = build(vec![local("/a", "~/r-a"), local("/a/b/c", "~/r-abc")]);
        assert_eq!(p.resolve("/a/b/c/f").expect("v").rule_prefix, "/a/b/c");
        assert_eq!(p.resolve("/a/b/f").expect("v").rule_prefix, "/a");
        assert_eq!(p.resolve("/a/file").expect("v").rule_prefix, "/a");
        assert_eq!(p.resolve("/other").expect("v").rule_prefix, "/");
    }

    // -- O2.3: component-boundary matching -----------------------------------

    #[test]
    fn prefix_matches_by_component_not_by_string() {
        let p = build(vec![local("/work/cache", "~/r")]);
        // "/work/cache-old" is OUTSIDE the rule — the classic string-prefix
        // bug would claim it.
        assert_eq!(p.resolve("/work/cache-old").expect("v").rule_prefix, "/");
        assert_eq!(
            p.resolve("/work/cache").expect("v").rule_prefix,
            "/work/cache"
        );
        assert_eq!(
            p.resolve("/work/cache/f").expect("v").rule_prefix,
            "/work/cache"
        );
    }

    // -- O2.4: path normalization (repeaters, ".", "..", trailing slash) -----

    #[test]
    fn normalization_collapses_before_matching() {
        let p = build(vec![local("/work/cache", "~/r")]);
        for q in [
            "//work///cache",
            "/work/./cache",
            "/work/other/../cache",
            "/work/cache/",
            "/work/cache/x/../",
        ] {
            let v = p.resolve(q).expect(q);
            assert_eq!(
                v.rule_prefix, "/work/cache",
                "query {q} should normalize into the rule"
            );
        }
    }

    #[test]
    fn dotdot_at_root_truncates_like_symlink_resolution() {
        let p = build(vec![local("/work/cache", "~/r")]);
        // "/../work/cache" pins to the root then descends — same contract as
        // core's symlink target resolution.
        assert_eq!(
            p.resolve("/../work/cache").expect("v").rule_prefix,
            "/work/cache"
        );
    }

    #[test]
    fn relative_paths_are_rejected_not_coerced() {
        let p = build(vec![]);
        assert!(matches!(
            p.resolve("work/cache"),
            Err(PolicyError::InvalidPath(_))
        ));
        assert!(matches!(p.resolve(""), Err(PolicyError::InvalidPath(_))));
        // rule prefixes go through the same gate
        let mut bad = local("work/cache", "~/r");
        bad.prefix = "work/cache".into();
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![bad])),
            Err(PolicyError::InvalidPath(_))
        ));
    }

    // -- O2.5: duplicate / conflicting rules are loud -------------------------

    #[test]
    fn duplicate_prefix_is_an_error_regardless_of_order() {
        for rules in [
            vec![local("/a", "~/r1"), local("/a", "~/r2")],
            vec![local("/a", "~/r2"), local("/a", "~/r1")],
        ] {
            let err = ResolvedPolicy::build(&section(rules)).unwrap_err();
            assert_eq!(err, PolicyError::DuplicatePrefix("/a".into()));
        }
    }

    #[test]
    fn equal_length_distinct_prefixes_are_fine() {
        let p = build(vec![local("/aa", "~/r1"), local("/bb", "~/r2")]);
        assert_eq!(p.resolve("/aa/f").expect("v").rule_prefix, "/aa");
        assert_eq!(p.resolve("/bb/f").expect("v").rule_prefix, "/bb");
    }

    #[test]
    fn explicit_root_rule_replaces_the_implicit_default_once() {
        // A user-written "/" rule is the explicit default; a second "/" is a
        // duplicate like any other.
        let s = section(vec![rule("/", "s3")]);
        let p = ResolvedPolicy::build(&s).expect("build");
        assert_eq!(p.resolve("/x").expect("v").rule_prefix, "/");
        assert_eq!(p.resolve("/x").expect("v").authority, Authority::Remote);
        assert_eq!(
            p.rules().count(),
            1,
            "explicit / replaces the implicit default"
        );
    }

    // -- O2.6: budget fields and global relationships -------------------------

    #[test]
    fn memory_cache_requires_memory_budget() {
        let mut r = cached_s3("/hot", "memory");
        r.disk_budget = Some("1GiB".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::MissingBudget {
                field: "memory_budget",
                ..
            })
        ));
    }

    #[test]
    fn disk_cache_requires_disk_budget_and_cache_root() {
        let mut r = cached_s3("/hot", "disk");
        r.disk_budget = Some("1GiB".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::MissingBudget {
                field: "cache_root",
                ..
            })
        ));
        let mut r_ok = cached_s3("/hot", "disk");
        r_ok.disk_budget = Some("1GiB".into());
        r_ok.cache_root = Some("~/.cache/x".into());
        let mut ok = section(vec![r_ok]);
        ok.limits = Some(PolicyLimits {
            memory_budget: None,
            disk_budget: Some("2GiB".into()),
        });
        // the shape is valid, but the FEATURE is not implemented: the gate
        // fires only after shape validation passes (P-R2)
        assert!(matches!(
            ResolvedPolicy::build(&ok),
            Err(PolicyError::CacheUnavailable { kind: "disk", .. })
        ));

        let mut r2 = cached_s3("/hot", "disk");
        r2.cache_root = Some("~/.cache/x".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r2])),
            Err(PolicyError::MissingBudget {
                field: "disk_budget",
                ..
            })
        ));
    }

    #[test]
    fn budgets_without_cache_are_rejected() {
        let mut r = rule("/hot", "s3");
        r.memory_budget = Some("1MiB".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::BudgetWithoutCache { .. })
        ));
    }

    #[test]
    fn per_rule_budget_needs_its_global_ceiling() {
        let mut r = cached_s3("/hot", "memory");
        r.memory_budget = Some("64MiB".into());
        // no limits at all
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r.clone()])),
            Err(PolicyError::MissingGlobalLimit { kind: "memory" })
        ));
        // ceiling present but smaller than the rule
        let mut s = section(vec![r.clone()]);
        s.limits = Some(PolicyLimits {
            memory_budget: Some("1MiB".into()),
            disk_budget: None,
        });
        assert!(matches!(
            ResolvedPolicy::build(&s),
            Err(PolicyError::BudgetOvercommit { kind: "memory" })
        ));
        // ceiling fits: shape validation passes, and since Q1 the memory
        // read cache is implemented — the config now BUILDS (the gate only
        // refuses unimplemented kinds/evictions)
        let mut ok = section(vec![r]);
        ok.limits = Some(PolicyLimits {
            memory_budget: Some("128MiB".into()),
            disk_budget: None,
        });
        assert!(
            ResolvedPolicy::build(&ok).is_ok(),
            "a well-formed memory-cache config must build (Q1)"
        );
    }

    #[test]
    fn zero_and_garbage_budgets_are_rejected() {
        assert!(matches!(
            parse_budget("x", "0"),
            Err(PolicyError::BadBudget { .. })
        ));
        assert!(matches!(
            parse_budget("x", "-5MiB"),
            Err(PolicyError::BadBudget { .. })
        ));
        assert!(matches!(
            parse_budget("x", "12MB"), // decimal units deliberately absent
            Err(PolicyError::BadBudget { .. })
        ));
        assert_eq!(parse_budget("x", "64MiB").expect("v"), 64 * (1 << 20));
        assert_eq!(parse_budget("x", "4096").expect("v"), 4096);
    }

    // -- O2.7: illegal Storage/Cache/Commit/Lifecycle combinations ------------

    #[test]
    fn client_local_with_any_cache_is_rejected() {
        for cache in ["memory", "disk", "memory+disk"] {
            let mut r = local("/scratch", "~/s");
            r.cache = Some(cache.to_string());
            let err = ResolvedPolicy::build(&section(vec![r])).unwrap_err();
            assert!(matches!(err, PolicyError::LocalWithCache { .. }), "{cache}");
        }
    }

    #[test]
    fn write_back_is_reserved_everywhere() {
        for storage in ["s3", "client-local"] {
            let mut r = rule("/x", storage);
            if storage == "client-local" {
                r.root = Some("~/s".into());
            }
            r.commit = Some("write-back".into());
            assert!(matches!(
                ResolvedPolicy::build(&section(vec![r])),
                Err(PolicyError::CommitReserved { .. })
            ));
        }
    }

    #[test]
    fn lifecycle_and_authority_life_are_storage_scoped() {
        // cache-eviction config on a cacheless local rule
        let mut r = local("/scratch", "~/s");
        r.lifecycle = Some("capacity".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::EvictionWithoutCache { .. })
        ));
        // authority_life on a remote rule
        let mut r2 = rule("/shared", "s3");
        r2.authority_life = Some("task".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r2])),
            Err(PolicyError::RemoteWithAuthorityLife { .. })
        ));
    }

    #[test]
    fn cross_field_fields_are_storage_scoped() {
        // root on an s3 rule
        let mut r = rule("/shared", "s3");
        r.root = Some("~/s".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::RootOnRemote { .. })
        ));
        // local rule without root
        let r2 = rule("/scratch", "client-local");
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r2])),
            Err(PolicyError::MissingRoot { .. })
        ));
        // cache budgets on a local rule
        let mut r3 = local("/scratch", "~/s");
        r3.memory_budget = Some("1MiB".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r3])),
            Err(PolicyError::BudgetWithoutCache { .. })
        ));
    }

    #[test]
    fn unknown_values_name_the_field() {
        let mut r = rule("/x", "nfs");
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r.clone()])),
            Err(PolicyError::UnknownValue {
                field: "storage",
                ..
            })
        ));
        r.storage = "s3".into();
        r.cache = Some("lru".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::UnknownValue { field: "cache", .. })
        ));
    }

    // -- O2.8: local root normalization and unsafe shapes ----------------------

    #[test]
    fn unsafe_local_roots_are_rejected_with_reasons() {
        for (root, why) in [
            ("", "empty"),
            ("relative/path", "relative"),
            ("/", "whole filesystem"),
            ("~", "whole home"),
            ("~/", "whole home"),
            ("~/a/../b", "traversal"),
            ("~/../escape", "traversal"),
        ] {
            let err = ResolvedPolicy::build(&section(vec![local("/s", root)])).unwrap_err();
            let PolicyError::UnsafeRoot { prefix, reason } = err else {
                panic!("{root}: expected UnsafeRoot");
            };
            assert_eq!(prefix, "/s");
            assert!(!reason.is_empty(), "{why}: reason must be named");
        }
    }

    #[test]
    fn two_local_rules_cannot_share_a_root() {
        let err = ResolvedPolicy::build(&section(vec![
            local("/s1", "~/same"),
            local("/s2", "~/same"),
        ]))
        .unwrap_err();
        assert!(matches!(err, PolicyError::UnsafeRoot { .. }));
    }

    #[test]
    fn sane_tilde_roots_are_accepted() {
        assert!(ResolvedPolicy::build(&section(vec![local("/s", "~/scratch/x")])).is_ok());
        assert!(ResolvedPolicy::build(&section(vec![local("/s", "/var/tmp/scratch")])).is_ok());
    }

    // -- O2.9: declaration order never matters ----------------------------------

    #[test]
    fn resolution_is_independent_of_declaration_order() {
        let remote = rule("/a/b", "s3");
        let rules_a = vec![local("/a", "~/r-a"), local("/a/b/c", "~/r-abc"), remote];
        let mk = |rules: Vec<RawRule>| ResolvedPolicy::build(&section(rules)).expect("build");
        let rules_b: Vec<RawRule> = rules_a.iter().rev().cloned().collect();
        let pa = mk(rules_a);
        let pb = mk(rules_b);
        for q in ["/a", "/a/f", "/a/b", "/a/b/f", "/a/b/c", "/a/b/c/f", "/z"] {
            assert_eq!(
                pa.resolve(q).expect("a"),
                pb.resolve(q).expect("b"),
                "order leaked into resolution for {q}"
            );
        }
    }

    #[test]
    fn view_shape_for_every_legal_combination() {
        // local rule
        let p = build(vec![local("/scratch", "~/s")]);
        let v = p.resolve("/scratch/f").expect("v");
        assert_eq!(v.authority, Authority::Local);
        assert_eq!(v.read_cache, CacheKind::None);
        assert_eq!(v.commit, CommitMode::StrictSync);
        assert_eq!(v.authority_life, AuthorityLife::Persistent);
        assert_eq!(v.cache_eviction, None);

        // task-scoped local authority: the MODEL parses it, the
        // availability gate refuses it until TaskScoped cleanup exists
        let mut r = local("/jobs", "~/j");
        r.authority_life = Some("task".into());
        assert!(matches!(
            ResolvedPolicy::build(&section(vec![r])),
            Err(PolicyError::TaskScopedUnavailable { prefix }) if prefix == "/jobs"
        ));

        // cached remote rule: model-valid, feature-unbuilt -> gate
        let mut r = cached_s3("/models", "memory+disk");
        r.memory_budget = Some("64MiB".into());
        r.disk_budget = Some("1GiB".into());
        r.cache_root = Some("~/.cache/m".into());
        r.lifecycle = Some("persistent".into());
        let mut s = section(vec![r]);
        s.limits = Some(PolicyLimits {
            memory_budget: Some("128MiB".into()),
            disk_budget: Some("2GiB".into()),
        });
        assert!(matches!(
            ResolvedPolicy::build(&s),
            Err(PolicyError::CacheUnavailable {
                kind: "memory+disk",
                ..
            })
        ));
    }

    // -- P-R2: unimplemented features are refused, never silently off ------
    // (Q1 narrowed the gate: cache = "memory" on the default capacity LRU
    // is now IMPLEMENTED and builds; disk / memory+disk still refuse.)

    #[test]
    fn unimplemented_cache_is_named_not_silently_ignored() {
        for (kind, want) in [("disk", "disk"), ("memory+disk", "memory+disk")] {
            let mut r = cached_s3("/hot", kind);
            if kind == "memory+disk" {
                r.memory_budget = Some("64MiB".into());
            }
            r.disk_budget = Some("1GiB".into());
            r.cache_root = Some("~/cache/hot".into());
            let mut s = section(vec![r]);
            s.limits = Some(PolicyLimits {
                memory_budget: Some("128MiB".into()),
                disk_budget: Some("2GiB".into()),
            });
            match ResolvedPolicy::build(&s) {
                Err(PolicyError::CacheUnavailable { prefix, kind: k }) => {
                    assert_eq!(prefix, "/hot");
                    assert_eq!(k, want);
                    let msg = PolicyError::CacheUnavailable {
                        prefix: "/hot".into(),
                        kind: k,
                    }
                    .to_string();
                    assert!(
                        msg.contains("no implementation"),
                        "message must say WHY: {msg}"
                    );
                }
                other => panic!("cache {kind}: expected CacheUnavailable, got {other:?}"),
            }
        }
    }

    // -- Q1: the opened combination and its named boundaries ----------------

    fn cached_mem(prefix: &str) -> RawRule {
        let mut r = cached_s3(prefix, "memory");
        r.memory_budget = Some("1MiB".into());
        r
    }

    /// A section with the mount-wide memory ceiling the cached rules need.
    fn mem_section(rules: Vec<RawRule>) -> PolicySection {
        let mut s = section(rules);
        s.limits = Some(PolicyLimits {
            memory_budget: Some("8MiB".into()),
            disk_budget: None,
        });
        s
    }

    #[test]
    fn memory_cache_default_and_explicit_capacity_build() {
        // default (no lifecycle) and explicit "capacity" are the SAME open
        // combination; the resolved view carries the implemented eviction
        // and the accessor exposes (prefix, budget) for the mount wiring.
        for lifecycle in [None, Some("capacity".to_string())] {
            let mut r = cached_mem("/hot");
            r.lifecycle = lifecycle;
            let p = ResolvedPolicy::build(&mem_section(vec![r])).expect("memory cache builds");
            let (prefix, budget) = p.memory_cache().expect("accessor sees the cached rule");
            assert_eq!(prefix, "/hot");
            assert_eq!(budget, 1 << 20);
            let v = p.resolve("/hot/f").unwrap();
            assert_eq!(v.read_cache, CacheKind::Memory);
            assert_eq!(v.cache_eviction, Some(CacheEviction::CapacityLru));
            assert_eq!(v.commit, CommitMode::StrictSync);
        }
    }

    #[test]
    fn memory_cache_without_global_limit_is_still_refused() {
        // the budget mechanism survives the gate unchanged: a rule budget
        // with no mount-wide ceiling stays an error (O-era check, not the
        // availability gate).
        let s = section(vec![cached_mem("/hot")]);
        match ResolvedPolicy::build(&s) {
            Err(PolicyError::MissingGlobalLimit { kind }) => assert_eq!(kind, "memory"),
            other => panic!("expected MissingGlobalLimit, got {other:?}"),
        }
    }

    #[test]
    fn unimplemented_eviction_is_named_not_silently_lru() {
        for (lifecycle, want) in [("persistent", "persistent"), ("task", "task")] {
            let mut r = cached_mem("/hot");
            r.lifecycle = Some(lifecycle.to_string());
            match ResolvedPolicy::build(&mem_section(vec![r])) {
                Err(PolicyError::EvictionUnavailable { prefix, kind }) => {
                    assert_eq!(prefix, "/hot");
                    assert_eq!(kind, want);
                    let msg = PolicyError::EvictionUnavailable {
                        prefix: "/hot".into(),
                        kind,
                    }
                    .to_string();
                    assert!(
                        msg.contains("single-tier memory read cache"),
                        "message must say WHY the LRU is not a silent downgrade: {msg}"
                    );
                }
                other => {
                    panic!("lifecycle {lifecycle}: expected EvictionUnavailable, got {other:?}")
                }
            }
        }
    }

    #[test]
    fn second_cached_rule_refuses_the_mount() {
        let a = cached_mem("/a");
        let b = cached_mem("/b");
        // declaration order must not matter (same gate discipline as P-R2)
        let e1 = ResolvedPolicy::build(&mem_section(vec![a.clone(), b.clone()])).unwrap_err();
        let e2 = ResolvedPolicy::build(&mem_section(vec![b, a])).unwrap_err();
        for e in [e1, e2] {
            match &e {
                PolicyError::TooManyCachedRules { prefixes } => {
                    assert_eq!(prefixes.len(), 2);
                    assert!(prefixes.contains(&"/a".to_string()));
                    assert!(prefixes.contains(&"/b".to_string()));
                }
                other => panic!("expected TooManyCachedRules, got {other:?}"),
            }
            assert!(
                e.to_string().contains("at most one memory-cached rule"),
                "message must say the limit: {e}"
            );
        }
    }

    #[test]
    fn availability_gate_is_order_independent() {
        // the same two rules in both orders: the gate fires identically
        // (Q1: "memory" now BUILDS, so the unavailable kind here is "disk")
        let mut a = cached_s3("/a", "disk");
        a.disk_budget = Some("1GiB".into());
        a.cache_root = Some("~/cache/a".into());
        let b = local("/b", "~/r-b");
        let mut s1 = section(vec![a.clone(), b.clone()]);
        s1.limits = Some(PolicyLimits {
            memory_budget: Some("8MiB".into()),
            disk_budget: Some("2GiB".into()),
        });
        let mut s2 = section(vec![b, a]);
        s2.limits = s1.limits.clone();
        let e1 = ResolvedPolicy::build(&s1).unwrap_err();
        let e2 = ResolvedPolicy::build(&s2).unwrap_err();
        assert_eq!(e1, e2, "the gate must not depend on declaration order");
    }
}
