//! Power-loss simulation for crash-consistency tests (T-P1-PL).
//!
//! Compiled only with the `simfs` feature (test builds); production code paths
//! call [`crate::fsutil::sync_file`] / [`crate::fsutil::fsync_dir`], which
//! notify this module only when the feature is enabled.
//!
//! # Model
//! While recording, every successful file fsync stores the file's content as
//! the *durable* content of its inode, and every directory fsync stores the
//! directory's entries (name → inode) as its *durable* listing. Immediately
//! before and after every fsync a [`CrashImage`] can be captured: the durable
//! state so far plus the current (page-cache) state of the tree.
//!
//! A crash image can be materialized into many possible post-power-loss disk
//! states ([`Variant`]):
//! * [`Variant::Durable`] — only what was fsynced survives;
//! * [`Variant::Current`] — everything survives (equivalent to a process crash);
//! * [`Variant::Random`] — independently per un-synced change:
//!   - each 512-byte sector whose content differs from the durable content is
//!     either the new data or the old data (zeros beyond the old length), so
//!     un-synced writes can be lost, torn at sector granularity, or persisted
//!     out of order (a later sector survives while an earlier one is lost);
//!   - the file length is either the durable or the current length;
//!   - un-synced creates, renames and unlinks are grouped into connected
//!     components (names ↔ inodes they touched); each component keeps either
//!     its durable or its current state, so a rename — including one that
//!     replaces an existing file — is atomic but not durable before the
//!     directory fsync;
//!   - each directory created or removed without a parent fsync may or may
//!     not exist.
//!
//! Assumptions (docs/storage-format/durability.md): fsync of a file makes all
//! of its previously written bytes durable; a 512-byte sector is written
//! atomically; unwritten regions read as zeros; storage honours flushes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Sector size of the torn-write model.
pub const SECTOR: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    File,
    Dir,
}

/// Directory listing: name → (inode, kind). Paths are relative to the root.
type Listing = BTreeMap<OsString, (u64, Kind)>;

#[derive(Debug, Default)]
struct State {
    root: PathBuf,
    durable_dirs: HashMap<PathBuf, Listing>,
    durable_files: HashMap<u64, Arc<Vec<u8>>>,
    tag: u64,
    images: Vec<CrashImage>,
    max_images: usize,
}

/// A captured crash point.
#[derive(Debug, Clone)]
pub struct CrashImage {
    /// Test-defined value at capture time (e.g. number of acknowledged writes).
    pub tag: u64,
    /// Where the image was taken, e.g. `before fsync wal/00..01.wal`.
    pub label: String,
    durable_dirs: HashMap<PathBuf, Listing>,
    durable_files: HashMap<u64, Arc<Vec<u8>>>,
    current_dirs: HashMap<PathBuf, Listing>,
    current_files: HashMap<u64, Arc<Vec<u8>>>,
}

/// Which post-crash disk state to build from a [`CrashImage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    Durable,
    Current,
    Random(u64),
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static STATE: Mutex<Option<State>> = Mutex::new(None);

fn lock() -> MutexGuard<'static, Option<State>> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

fn rel(root: &Path, p: &Path) -> Option<PathBuf> {
    p.strip_prefix(root).ok().map(Path::to_path_buf)
}

fn list(dir: &Path) -> Listing {
    let mut out = Listing::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let Ok(md) = fs::symlink_metadata(e.path()) else {
            continue;
        };
        let kind = if md.is_dir() {
            Kind::Dir
        } else if md.is_file() {
            Kind::File
        } else {
            continue;
        };
        out.insert(e.file_name(), (md.ino(), kind));
    }
    out
}

/// Walk the current tree: (dir listings, file contents by inode).
type Tree = (HashMap<PathBuf, Listing>, HashMap<u64, Arc<Vec<u8>>>);

fn walk(root: &Path) -> Tree {
    let mut dirs = HashMap::new();
    let mut files = HashMap::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(r) = stack.pop() {
        let l = list(&root.join(&r));
        for (name, (ino, kind)) in &l {
            match kind {
                Kind::Dir => stack.push(r.join(name)),
                Kind::File => {
                    if let Ok(b) = fs::read(root.join(&r).join(name)) {
                        files.insert(*ino, Arc::new(b));
                    }
                }
            }
        }
        dirs.insert(r, l);
    }
    (dirs, files)
}

/// Start recording under `root`. The tree as it exists now is treated as
/// durable. At most `max_images` crash images are kept.
pub fn enable(root: &Path, max_images: usize) {
    let (durable_dirs, durable_files) = walk(root);
    *lock() = Some(State {
        root: root.to_path_buf(),
        durable_dirs,
        durable_files,
        tag: 0,
        images: Vec::new(),
        max_images,
    });
    ENABLED.store(true, Ordering::SeqCst);
}

/// Stop recording and return the captured images.
pub fn disable() -> Vec<CrashImage> {
    ENABLED.store(false, Ordering::SeqCst);
    lock().take().map(|s| s.images).unwrap_or_default()
}

/// Set the tag stored in subsequently captured images.
pub fn set_tag(tag: u64) {
    if let Some(s) = lock().as_mut() {
        s.tag = tag;
    }
}

fn with_state(f: impl FnOnce(&mut State)) {
    if !ENABLED.load(Ordering::SeqCst) {
        return;
    }
    if let Some(s) = lock().as_mut() {
        f(s);
    }
}

/// Capture a crash image now (also called around every fsync).
pub fn crash_point(label: &str) {
    with_state(|s| {
        if s.images.len() >= s.max_images {
            return;
        }
        let (current_dirs, current_files) = walk(&s.root);
        s.images.push(CrashImage {
            tag: s.tag,
            label: label.to_owned(),
            durable_dirs: s.durable_dirs.clone(),
            durable_files: s.durable_files.clone(),
            current_dirs,
            current_files,
        });
    });
}

/// A new inode was created at `path` (its durable content is empty).
pub fn file_created(path: &Path) {
    with_state(|s| {
        if rel(&s.root, path).is_some() {
            if let Ok(md) = fs::metadata(path) {
                s.durable_files.remove(&md.ino());
            }
        }
    });
}

/// Called before and after fsync of the file at `path`; `synced` is true
/// after a successful fsync.
pub fn file_sync(path: &Path, synced: bool) {
    let label = |s: &State| {
        format!(
            "{} fsync {}",
            if synced { "after" } else { "before" },
            rel(&s.root, path).unwrap_or_default().display()
        )
    };
    let inside = {
        let g = lock();
        g.as_ref().is_some_and(|s| rel(&s.root, path).is_some())
    };
    if !inside || !ENABLED.load(Ordering::SeqCst) {
        return;
    }
    if synced {
        with_state(|s| {
            if let (Ok(md), Ok(b)) = (fs::metadata(path), fs::read(path)) {
                s.durable_files.insert(md.ino(), Arc::new(b));
            }
        });
    }
    let l = lock().as_ref().map(label).unwrap_or_default();
    crash_point(&l);
}

/// Called before and after fsync of directory `dir`.
pub fn dir_sync(dir: &Path, synced: bool) {
    if !ENABLED.load(Ordering::SeqCst) {
        return;
    }
    let r = {
        let g = lock();
        g.as_ref().and_then(|s| rel(&s.root, dir))
    };
    let Some(r) = r else { return };
    if synced {
        with_state(|s| {
            s.durable_dirs.insert(r.clone(), list(dir));
        });
    }
    crash_point(&format!(
        "{} fsync dir {}/",
        if synced { "after" } else { "before" },
        r.display()
    ));
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

/// Directories reachable from the root through `dirs` listings.
fn tree_dirs(dirs: &HashMap<PathBuf, Listing>) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(d) = stack.pop() {
        if let Some(l) = dirs.get(&d) {
            for (name, (_, kind)) in l {
                if *kind == Kind::Dir {
                    stack.push(d.join(name));
                }
            }
        }
        out.insert(d);
    }
    out
}

/// File names per inode among `present` directories.
fn names(
    dirs: &HashMap<PathBuf, Listing>,
    present: &BTreeSet<PathBuf>,
) -> BTreeMap<u64, BTreeSet<PathBuf>> {
    let mut out: BTreeMap<u64, BTreeSet<PathBuf>> = BTreeMap::new();
    for d in present {
        if let Some(l) = dirs.get(d) {
            for (name, (ino, kind)) in l {
                if *kind == Kind::File {
                    out.entry(*ino).or_default().insert(d.join(name));
                }
            }
        }
    }
    out
}

/// Name → inode for files in `present` directories.
fn file_names(
    dirs: &HashMap<PathBuf, Listing>,
    present: &BTreeSet<PathBuf>,
) -> BTreeMap<PathBuf, u64> {
    let mut out = BTreeMap::new();
    for (ino, ns) in names(dirs, present) {
        for n in ns {
            out.insert(n, ino);
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Node {
    Name(PathBuf),
    Ino(u64),
}

#[derive(Default)]
struct UnionFind {
    ids: HashMap<Node, usize>,
    parent: Vec<usize>,
}

impl UnionFind {
    fn node(&mut self, n: Node) -> usize {
        let next = self.parent.len();
        let id = *self.ids.entry(n).or_insert(next);
        if id == next {
            self.parent.push(next);
        }
        id
    }
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }
    fn find_node(&mut self, n: &Node) -> usize {
        let id = self.ids.get(n).copied().unwrap_or(usize::MAX);
        if id == usize::MAX {
            return usize::MAX;
        }
        self.find(id)
    }
    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

fn mix(old: &[u8], new: &[u8], rng: &mut Rng) -> Vec<u8> {
    let len = if rng.coin() { old.len() } else { new.len() };
    let mut out = vec![0u8; len];
    let sector = |b: &[u8], i: usize| -> Vec<u8> {
        let mut s = vec![0u8; SECTOR];
        let lo = (i * SECTOR).min(b.len());
        let hi = ((i + 1) * SECTOR).min(b.len());
        s[..hi - lo].copy_from_slice(&b[lo..hi]);
        s
    };
    for i in 0..len.div_ceil(SECTOR) {
        let (o, n) = (sector(old, i), sector(new, i));
        let pick = if o == n || rng.coin() { n } else { o };
        let lo = i * SECTOR;
        let hi = (lo + SECTOR).min(len);
        out[lo..hi].copy_from_slice(&pick[..hi - lo]);
    }
    out
}

impl CrashImage {
    /// Write the disk state for `variant` into the empty directory `out`.
    pub fn materialize(&self, variant: Variant, out: &Path) -> std::io::Result<()> {
        let mut rng = Rng(match variant {
            Variant::Random(seed) => seed ^ 0x5157_4f46,
            _ => 0,
        });
        let dur_dirs = tree_dirs(&self.durable_dirs);
        let cur_dirs = tree_dirs(&self.current_dirs);
        // Directories: present in both → present; in one → per variant / coin.
        let mut dirs = BTreeSet::new();
        for d in dur_dirs.union(&cur_dirs) {
            let parent_ok = d.parent().is_none_or(|p| dirs.contains(p)) || d.as_os_str().is_empty();
            if !parent_ok {
                continue;
            }
            let keep = match (dur_dirs.contains(d), cur_dirs.contains(d), variant) {
                (true, true, _) => true,
                (a, _, Variant::Durable) => a,
                (_, b, Variant::Current) => b,
                (_, _, Variant::Random(_)) => rng.coin(),
            };
            if keep {
                dirs.insert(d.clone());
            }
        }
        for d in &dirs {
            fs::create_dir_all(out.join(d))?;
        }
        // Files. Names and inodes touched by un-synced metadata changes are
        // grouped into connected components (a rename links the old name, the
        // new name, the moved inode and any replaced inode); each component
        // keeps either its durable or its current state as a unit, so renames
        // (including replace-renames) stay atomic.
        let dn = file_names(&self.durable_dirs, &dur_dirs);
        let cn = file_names(&self.current_dirs, &cur_dirs);
        let mut uf = UnionFind::default();
        let all: BTreeSet<&PathBuf> = dn.keys().chain(cn.keys()).collect();
        for name in &all {
            let (d, c) = (dn.get(*name), cn.get(*name));
            if d != c {
                let n = uf.node(Node::Name((*name).clone()));
                for ino in d.into_iter().chain(c) {
                    let i = uf.node(Node::Ino(*ino));
                    uf.union(n, i);
                }
            }
        }
        let nd = names(&self.durable_dirs, &dur_dirs);
        let nc = names(&self.current_dirs, &cur_dirs);
        let empty = BTreeSet::new();
        for ino in nd.keys().chain(nc.keys()) {
            let (a, b) = (nd.get(ino).unwrap_or(&empty), nc.get(ino).unwrap_or(&empty));
            if a != b {
                let i = uf.node(Node::Ino(*ino));
                for name in a.iter().chain(b) {
                    let n = uf.node(Node::Name(name.clone()));
                    uf.union(i, n);
                }
            }
        }
        let mut use_current: HashMap<usize, bool> = HashMap::new();
        let mut chosen: BTreeMap<PathBuf, u64> = BTreeMap::new();
        for name in all {
            let (d, c) = (dn.get(name), cn.get(name));
            let pick = if d == c {
                d
            } else {
                let root = uf.find_node(&Node::Name(name.clone()));
                let cur = *use_current.entry(root).or_insert_with(|| match variant {
                    Variant::Durable => false,
                    Variant::Current => true,
                    Variant::Random(_) => rng.coin(),
                });
                if cur {
                    c
                } else {
                    d
                }
            };
            if let Some(ino) = pick {
                chosen.insert(name.clone(), *ino);
            }
        }
        let mut contents: HashMap<u64, Vec<u8>> = HashMap::new();
        for (name, ino) in chosen {
            if !name.parent().is_some_and(|d| dirs.contains(d)) {
                continue;
            }
            let content = contents.entry(ino).or_insert_with(|| {
                let old = self
                    .durable_files
                    .get(&ino)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                match (variant, self.current_files.get(&ino)) {
                    (Variant::Durable, _) | (_, None) => old.to_vec(),
                    (Variant::Current, Some(new)) => new.to_vec(),
                    (Variant::Random(_), Some(new)) => mix(old, new, &mut rng),
                }
            });
            fs::write(out.join(&name), &*content)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::io::Write;

    // The recorder is process-global.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn read(p: &Path) -> Vec<u8> {
        fs::read(p).unwrap_or_default()
    }

    #[test]
    fn durable_current_and_random_states() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("db");
        fs::create_dir(&root).unwrap();
        enable(&root, 100);
        // a: synced 600 bytes, then 600 more unsynced bytes.
        let pa = root.join("a");
        let mut f = crate::fsutil::create_new_file(&pa).unwrap();
        f.write_all(&[1u8; 600]).unwrap();
        crate::fsutil::sync_file(&f, &pa, false).unwrap();
        crate::fsutil::fsync_dir(&root).unwrap();
        f.write_all(&[2u8; 600]).unwrap();
        // b: renamed from b.tmp, directory not yet fsynced.
        fs::write(root.join("b.tmp"), b"bee").unwrap();
        let fb = fs::File::open(root.join("b.tmp")).unwrap();
        crate::fsutil::sync_file(&fb, &root.join("b.tmp"), false).unwrap();
        fs::rename(root.join("b.tmp"), root.join("b")).unwrap();
        crash_point("end");
        let imgs = disable();
        let img = imgs.iter().find(|i| i.label == "end").unwrap();

        let d = dir.path().join("durable");
        img.materialize(Variant::Durable, &d).unwrap();
        assert_eq!(read(&d.join("a")), vec![1u8; 600]);
        assert!(!d.join("b").exists() && !d.join("b.tmp").exists());

        let c = dir.path().join("current");
        img.materialize(Variant::Current, &c).unwrap();
        assert_eq!(read(&c.join("a")).len(), 1200);
        assert_eq!(read(&c.join("b")), b"bee");
        assert!(!c.join("b.tmp").exists());

        let mut saw_reorder = false;
        for seed in 0..64 {
            let r = dir.path().join(format!("r{seed}"));
            img.materialize(Variant::Random(seed), &r).unwrap();
            let a = read(&r.join("a"));
            assert!(a.len() == 600 || a.len() == 1200);
            assert_eq!(&a[..512], &[1u8; 512][..], "synced sector must survive");
            // b never appears under both names, nor under the tmp name.
            assert!(!(r.join("b").exists() && r.join("b.tmp").exists()));
            assert!(!r.join("b.tmp").exists());
            if a.len() == 1200 && a[1100] == 2 && a[700] == 0 {
                saw_reorder = true; // later sector survived, earlier lost
            }
        }
        assert!(saw_reorder, "random variants must include reordered writes");

        // Replace-rename: `m` always holds the old or the new content.
        let _g2 = ();
        let root2 = dir.path().join("db2");
        fs::create_dir(&root2).unwrap();
        fs::write(root2.join("m"), b"old").unwrap();
        enable(&root2, 10);
        crate::fsutil::atomic_write(&root2.join("m"), b"new", "t").unwrap_or(());
        let imgs = disable();
        assert!(imgs.len() >= 4);
        for (k, img) in imgs.iter().enumerate() {
            for seed in 0..32 {
                let r = dir.path().join(format!("m{k}-{seed}"));
                img.materialize(Variant::Random(seed), &r).unwrap();
                let m = read(&r.join("m"));
                assert!(m == b"old" || m == b"new", "{} {seed}: {m:?}", img.label);
            }
        }
    }
}
