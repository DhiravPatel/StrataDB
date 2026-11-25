//! Durable filesystem primitives.
//!
//! Platform assumptions (documented in docs/storage-format/durability.md):
//! * `rename(2)` within one directory is atomic with respect to crashes on
//!   APFS, ext4, and XFS.
//! * A rename or file creation is only durable once the *parent directory*
//!   has been fsynced.
//! * `File::sync_all`/`sync_data` on Apple platforms issue `F_FULLFSYNC`
//!   (Rust std behaviour), which flushes the drive write cache. On Linux they
//!   call `fsync`/`fdatasync`.
//! * After an fsync failure the page-cache state is unknown; callers must
//!   treat the file as poisoned and never retry the fsync as if it succeeded.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::error::{Error, IoResultExt, Result};

/// Permission bits for data files: owner read/write only (spec §18.9).
pub const FILE_MODE: u32 = 0o600;
/// Permission bits for data directories: owner only.
pub const DIR_MODE: u32 = 0o700;

/// Fsync a directory so that entries created, renamed, or removed in it are durable.
pub fn fsync_dir(dir: &Path) -> Result<()> {
    #[cfg(feature = "simfs")]
    crate::simfs::dir_sync(dir, false);
    let f = File::open(dir).ctx(|| format!("opening directory {} for fsync", dir.display()))?;
    f.sync_all()
        .ctx(|| format!("fsyncing directory {}", dir.display()))?;
    #[cfg(feature = "simfs")]
    crate::simfs::dir_sync(dir, true);
    Ok(())
}

/// Fsync file data (`data_only`: `sync_data`, else `sync_all`). Every fsync
/// of a data file goes through here so power-loss simulation can observe it.
/// `path` is the file's current name (used only by the simulator).
pub fn sync_file(f: &File, path: &Path, data_only: bool) -> std::io::Result<()> {
    #[cfg(feature = "simfs")]
    crate::simfs::file_sync(path, false);
    #[cfg(not(feature = "simfs"))]
    let _ = path;
    if data_only {
        f.sync_data()?;
    } else {
        f.sync_all()?;
    }
    #[cfg(feature = "simfs")]
    crate::simfs::file_sync(path, true);
    Ok(())
}

/// Create `dir` (and parents) with owner-only permissions, fsyncing the parent
/// of every directory that had to be created.
pub fn create_dir_durable(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    if let Some(parent) = dir.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_durable(parent)?;
        }
    }
    match fs::DirBuilder::new().mode(DIR_MODE).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && dir.is_dir() => return Ok(()),
        Err(e) => {
            return Err(Error::io(
                format!("creating directory {}", dir.display()),
                e,
            ))
        }
    }
    if let Some(parent) = dir.parent() {
        if !parent.as_os_str().is_empty() {
            fsync_dir(parent)?;
        }
    }
    Ok(())
}

/// Path of the temporary sibling used by [`atomic_write`].
pub fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Create a new file for writing with owner-only permissions. Fails if it exists.
pub fn create_new_file(path: &Path) -> Result<File> {
    let f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .open(path)
        .ctx(|| format!("creating {}", path.display()))?;
    #[cfg(feature = "simfs")]
    crate::simfs::file_created(path);
    Ok(f)
}

/// Atomically replace `path` with `contents`:
/// write `path.tmp`, fsync it, rename over `path`, fsync the parent directory.
///
/// After a crash, `path` holds either the complete old contents or the complete
/// new contents. A stale `path.tmp` may remain and must be ignored/removed by
/// the caller's recovery logic.
pub fn atomic_write(path: &Path, contents: &[u8], failpoint_prefix: &str) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::internal(format!("{} has no parent", path.display())))?;
    let tmp = tmp_path(path);
    remove_if_exists(&tmp)?;
    let mut f = create_new_file(&tmp)?;
    f.write_all(contents)
        .ctx(|| format!("writing {}", tmp.display()))?;
    sync_file(&f, &tmp, false).ctx(|| format!("fsyncing {}", tmp.display()))?;
    drop(f);
    crate::failpoint::hit(&format!("{failpoint_prefix}::before_rename"))?;
    fs::rename(&tmp, path).ctx(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
    crate::failpoint::hit(&format!("{failpoint_prefix}::after_rename"))?;
    fsync_dir(parent)
}

/// Remove a file, treating "not found" as success.
pub fn remove_if_exists(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::io(format!("removing {}", path.display()), e)),
    }
}

/// List regular files in `dir` (non-recursive), sorted by name.
pub fn list_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).ctx(|| format!("listing {}", dir.display()))? {
        let entry = entry.ctx(|| format!("reading entry in {}", dir.display()))?;
        let ft = entry
            .file_type()
            .ctx(|| format!("stat {}", entry.path().display()))?;
        // Symlinks are deliberately not followed (spec §18.1 symlink attacks).
        if ft.is_file() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// List subdirectories of `dir`, sorted by name. Symlinks are ignored.
pub fn list_dirs(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).ctx(|| format!("listing {}", dir.display()))? {
        let entry = entry.ctx(|| format!("reading entry in {}", dir.display()))?;
        let ft = entry
            .file_type()
            .ctx(|| format!("stat {}", entry.path().display()))?;
        if ft.is_dir() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn atomic_write_replaces_contents() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("MANIFEST");
        atomic_write(&p, b"one", "test").unwrap();
        atomic_write(&p, b"two", "test").unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"two");
        assert!(!tmp_path(&p).exists());
        let mode = fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, FILE_MODE);
    }

    #[test]
    fn atomic_write_tolerates_stale_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("X");
        fs::write(tmp_path(&p), b"garbage").unwrap();
        atomic_write(&p, b"ok", "test").unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"ok");
    }

    #[test]
    fn create_dir_durable_sets_mode() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("a/b/c");
        create_dir_durable(&d).unwrap();
        let mode = fs::metadata(&d).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, DIR_MODE);
        create_dir_durable(&d).unwrap();
    }

    #[test]
    fn fsync_dir_works_on_this_platform() {
        let dir = tempfile::tempdir().unwrap();
        fsync_dir(dir.path()).unwrap();
    }
}
