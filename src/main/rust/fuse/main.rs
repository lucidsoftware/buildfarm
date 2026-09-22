// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fuser::{
    AccessFlags, BackingId, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, InitFlags, KernelConfig, LockOwner, MountOption, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PROTOCOL_VERSION: u32 = 1;
const COMMAND_ADD_ROOT: u8 = 1;
const COMMAND_REMOVE_ROOT: u8 = 2;
const COMMAND_SHUTDOWN: u8 = 3;
const ENTRY_DIRECTORY: u8 = 1;
const ENTRY_FILE: u8 = 2;
const ENTRY_SYMLINK: u8 = 3;
const IMMUTABLE_TTL: Duration = Duration::from_secs(3600);
const MUTABLE_TTL: Duration = Duration::ZERO;

#[derive(Debug)]
enum NodeKind {
    Directory(BTreeMap<OsString, u64>),
    InputFile { path: Option<PathBuf> },
    OutputFile { path: PathBuf },
    Symlink(OsString),
}

#[derive(Debug)]
struct Node {
    parent: u64,
    kind: NodeKind,
    perm: u16,
    size: u64,
    nlink: u32,
    immutable: bool,
}

impl Node {
    fn file_type(&self) -> FileType {
        match self.kind {
            NodeKind::Directory(_) => FileType::Directory,
            NodeKind::InputFile { .. } | NodeKind::OutputFile { .. } => FileType::RegularFile,
            NodeKind::Symlink(_) => FileType::Symlink,
        }
    }

    fn writable(&self) -> bool {
        matches!(self.kind, NodeKind::Directory(_) | NodeKind::OutputFile { .. })
    }
}

#[derive(Debug)]
struct State {
    nodes: HashMap<u64, Node>,
    next_inode: u64,
}

impl State {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            INodeNo::ROOT.0,
            Node {
                parent: INodeNo::ROOT.0,
                kind: NodeKind::Directory(BTreeMap::new()),
                perm: 0o755,
                size: 0,
                nlink: 2,
                immutable: false,
            },
        );
        Self {
            nodes,
            next_inode: 2,
        }
    }

    fn allocate_inode(&mut self) -> u64 {
        let ino = self.next_inode;
        self.next_inode = self.next_inode.checked_add(1).expect("inode space exhausted");
        ino
    }

    fn child(&self, parent: u64, name: &OsStr) -> Option<u64> {
        match &self.nodes.get(&parent)?.kind {
            NodeKind::Directory(children) => children.get(name).copied(),
            _ => None,
        }
    }

    fn insert_child(&mut self, parent: u64, name: OsString, node: Node) -> Result<u64, Errno> {
        let directory = self.nodes.get(&parent).ok_or(Errno::ENOENT)?;
        let NodeKind::Directory(children) = &directory.kind else {
            return Err(Errno::ENOTDIR);
        };
        if children.contains_key(&name) {
            return Err(Errno::EEXIST);
        }
        let ino = self.allocate_inode();
        let NodeKind::Directory(children) = &mut self.nodes.get_mut(&parent).unwrap().kind else {
            unreachable!();
        };
        children.insert(name, ino);
        self.nodes.insert(ino, node);
        Ok(ino)
    }

    fn remove_tree(&mut self, ino: u64) {
        let children = self.nodes.get(&ino).and_then(|node| match &node.kind {
            NodeKind::Directory(children) => Some(children.values().copied().collect::<Vec<_>>()),
            _ => None,
        });
        if let Some(children) = children {
            for child in children {
                self.remove_tree(child);
            }
        }
        if let Some(Node {
            kind: NodeKind::OutputFile { path },
            ..
        }) = self.nodes.remove(&ino)
        {
            let _ = fs::remove_file(path);
        }
    }

    fn drop_link(&mut self, ino: u64) {
        let remove = match self.nodes.get_mut(&ino) {
            Some(node) => {
                node.nlink = node.nlink.saturating_sub(1);
                node.nlink == 0
            }
            None => false,
        };
        if remove {
            self.remove_tree(ino);
        }
    }

    fn is_ancestor(&self, ancestor: u64, mut ino: u64) -> bool {
        loop {
            if ino == ancestor {
                return true;
            }
            let Some(node) = self.nodes.get(&ino) else {
                return false;
            };
            if node.parent == ino {
                return false;
            }
            ino = node.parent;
        }
    }

    fn resolve_relative(&self, root: u64, path: &Path) -> Result<u64, String> {
        let mut current = root;
        for component in path.components() {
            let Component::Normal(name) = component else {
                return Err(format!("invalid relative path: {}", path.display()));
            };
            current = self
                .child(current, name)
                .ok_or_else(|| format!("missing parent for {}", path.display()))?;
        }
        Ok(current)
    }
}

#[derive(Debug)]
struct Handle {
    file: File,
    writable: bool,
}

#[derive(Debug, Default)]
struct BackingCache {
    by_handle: HashMap<u64, Arc<BackingId>>,
    by_inode: HashMap<u64, Weak<BackingId>>,
}

#[derive(Debug)]
struct BuildfarmFuse {
    state: Arc<RwLock<State>>,
    handles: RwLock<HashMap<u64, Handle>>,
    backings: Mutex<BackingCache>,
    next_handle: AtomicU64,
    passthrough: AtomicBool,
    scratch: PathBuf,
    uid: u32,
    gid: u32,
}

impl BuildfarmFuse {
    fn new(state: Arc<RwLock<State>>, scratch: PathBuf) -> Self {
        Self {
            state,
            handles: RwLock::new(HashMap::new()),
            backings: Mutex::new(BackingCache::default()),
            next_handle: AtomicU64::new(1),
            passthrough: AtomicBool::new(false),
            scratch,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        }
    }

    fn attr(&self, ino: u64, node: &Node) -> FileAttr {
        let size = match &node.kind {
            NodeKind::OutputFile { path } => fs::metadata(path).map_or(node.size, |metadata| metadata.len()),
            _ => node.size,
        };
        FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512),
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: node.file_type(),
            perm: node.perm,
            nlink: node.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn ttl(node: &Node) -> &'static Duration {
        if node.immutable {
            &IMMUTABLE_TTL
        } else {
            &MUTABLE_TTL
        }
    }

    fn open_node(&self, ino: u64, flags: i32) -> Result<u64, Errno> {
        let state = self.state.read().unwrap();
        let node = state.nodes.get(&ino).ok_or(Errno::ENOENT)?;
        if matches!(node.kind, NodeKind::Directory(_)) {
            return Err(Errno::EISDIR);
        }
        if matches!(node.kind, NodeKind::Symlink(_)) {
            return Err(Errno::EINVAL);
        }
        let wants_write = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        let path = match &node.kind {
            NodeKind::InputFile { path } => {
                if wants_write {
                    return Err(Errno::EPERM);
                }
                path.clone()
                    .unwrap_or_else(|| self.scratch.join("empty-input"))
            }
            NodeKind::OutputFile { path } => path.clone(),
            _ => unreachable!(),
        };
        let mut options = OpenOptions::new();
        options.read(!wants_write || (flags & libc::O_ACCMODE) == libc::O_RDWR);
        options.write(wants_write);
        options.append(flags & libc::O_APPEND != 0);
        let file = options.open(path).map_err(Errno::from)?;
        drop(state);
        let fh = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.handles
            .write()
            .unwrap()
            .insert(fh, Handle { file, writable: wants_write });
        Ok(fh)
    }

    fn create_output(&self, parent: u64, name: &OsStr, mode: u32) -> Result<u64, Errno> {
        fs::create_dir_all(&self.scratch).map_err(Errno::from)?;
        let mut state = self.state.write().unwrap();
        if state.child(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        if !matches!(state.nodes.get(&parent).map(|n| &n.kind), Some(NodeKind::Directory(_))) {
            return Err(Errno::ENOENT);
        }
        let ino = state.next_inode;
        let path = self.scratch.join(format!("output-{ino}"));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(Errno::from)?;
        state.insert_child(
            parent,
            name.to_os_string(),
            Node {
                parent,
                kind: NodeKind::OutputFile { path },
                perm: (mode as u16) & 0o777,
                size: 0,
                nlink: 1,
                immutable: false,
            },
        )
    }

    fn truncate(&self, ino: u64, size: u64, fh: Option<FileHandle>) -> Result<(), Errno> {
        if let Some(fh) = fh {
            let handles = self.handles.read().unwrap();
            let handle = handles.get(&fh.0).ok_or(Errno::EBADF)?;
            if !handle.writable {
                return Err(Errno::EPERM);
            }
            handle.file.set_len(size).map_err(Errno::from)?;
        } else {
            let state = self.state.read().unwrap();
            let node = state.nodes.get(&ino).ok_or(Errno::ENOENT)?;
            let NodeKind::OutputFile { path } = &node.kind else {
                return Err(if matches!(node.kind, NodeKind::Directory(_)) {
                    Errno::EISDIR
                } else {
                    Errno::EPERM
                });
            };
            OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|file| file.set_len(size))
                .map_err(Errno::from)?;
        }
        let mut state = self.state.write().unwrap();
        if let Some(node) = state.nodes.get_mut(&ino) {
            node.size = size;
        }
        Ok(())
    }

    fn open_passthrough(
        &self,
        ino: u64,
        fh: u64,
        reply: &ReplyOpen,
    ) -> io::Result<Arc<BackingId>> {
        let file = {
            let handles = self.handles.read().unwrap();
            handles
                .get(&fh)
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
                .file
                .try_clone()?
        };
        let mut cache = self.backings.lock().unwrap();
        if let Some(backing) = cache.by_inode.get(&ino).and_then(Weak::upgrade) {
            cache.by_handle.insert(fh, Arc::clone(&backing));
            return Ok(backing);
        }
        let backing = Arc::new(reply.open_backing(&file)?);
        cache.by_inode.insert(ino, Arc::downgrade(&backing));
        cache.by_handle.insert(fh, Arc::clone(&backing));
        Ok(backing)
    }
}

impl Filesystem for BuildfarmFuse {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let _ = config.set_max_write(1024 * 1024);
        let _ = config.set_max_readahead(1024 * 1024);
        let _ = config.set_max_background(256);
        let _ = config.set_congestion_threshold(192);
        if config.capabilities().contains(InitFlags::FUSE_PASSTHROUGH)
            && config
                .add_capabilities(InitFlags::FUSE_PASSTHROUGH)
                .is_ok()
            && config.set_max_stack_depth(2).is_ok()
        {
            self.passthrough.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let state = self.state.read().unwrap();
        let Some(ino) = state.child(parent.0, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let node = &state.nodes[&ino];
        reply.entry(Self::ttl(node), &self.attr(ino, node), Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let state = self.state.read().unwrap();
        match state.nodes.get(&ino.0) {
            Some(node) => reply.attr(Self::ttl(node), &self.attr(ino.0, node)),
            None => reply.error(Errno::ENOENT),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        if uid.is_some_and(|value| value != self.uid) || gid.is_some_and(|value| value != self.gid) {
            reply.error(Errno::EPERM);
            return;
        }
        if let Some(size) = size {
            if let Err(error) = self.truncate(ino.0, size, fh) {
                reply.error(error);
                return;
            }
        }
        let mut state = self.state.write().unwrap();
        let Some(node) = state.nodes.get_mut(&ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if let Some(mode) = mode {
            if !node.writable() || matches!(node.kind, NodeKind::Directory(_)) {
                reply.error(Errno::EPERM);
                return;
            }
            node.perm = (mode as u16) & 0o777;
        }
        reply.attr(Self::ttl(node), &self.attr(ino.0, node));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let state = self.state.read().unwrap();
        match state.nodes.get(&ino.0).map(|node| &node.kind) {
            Some(NodeKind::Symlink(target)) => reply.data(target.as_bytes()),
            Some(_) => reply.error(Errno::EINVAL),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let mut state = self.state.write().unwrap();
        let node = Node {
            parent: parent.0,
            kind: NodeKind::Symlink(target.as_os_str().to_os_string()),
            perm: 0o777,
            size: target.as_os_str().as_bytes().len() as u64,
            nlink: 1,
            immutable: false,
        };
        match state.insert_child(parent.0, name.to_os_string(), node) {
            Ok(ino) => reply.entry(&MUTABLE_TTL, &self.attr(ino, &state.nodes[&ino]), Generation(0)),
            Err(error) => reply.error(error),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let mut state = self.state.write().unwrap();
        let node = Node {
            parent: parent.0,
            kind: NodeKind::Directory(BTreeMap::new()),
            perm: ((mode & !umask) as u16) & 0o777,
            size: 0,
            nlink: 2,
            immutable: false,
        };
        match state.insert_child(parent.0, name.to_os_string(), node) {
            Ok(ino) => reply.entry(&MUTABLE_TTL, &self.attr(ino, &state.nodes[&ino]), Generation(0)),
            Err(error) => reply.error(error),
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        match self.create_output(parent.0, name, mode) {
            Ok(ino) => {
                let state = self.state.read().unwrap();
                reply.entry(&MUTABLE_TTL, &self.attr(ino, &state.nodes[&ino]), Generation(0));
            }
            Err(error) => reply.error(error),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let ino = match self.create_output(parent.0, name, mode & !umask) {
            Ok(ino) => ino,
            Err(error) => {
                reply.error(error);
                return;
            }
        };
        match self.open_node(ino, flags) {
            Ok(fh) => {
                let state = self.state.read().unwrap();
                reply.created(
                    &MUTABLE_TTL,
                    &self.attr(ino, &state.nodes[&ino]),
                    Generation(0),
                    FileHandle(fh),
                    FopenFlags::empty(),
                );
            }
            Err(error) => reply.error(error),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        match self.open_node(ino.0, flags.0) {
            Ok(fh) => {
                if self.passthrough.load(Ordering::Relaxed) {
                    if let Ok(backing) = self.open_passthrough(ino.0, fh, &reply) {
                        reply.opened_passthrough(
                            FileHandle(fh),
                            FopenFlags::FOPEN_KEEP_CACHE,
                            &backing,
                        );
                        return;
                    }
                }
                reply.opened(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE);
            }
            Err(error) => reply.error(error),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let handles = self.handles.read().unwrap();
        let Some(handle) = handles.get(&fh.0) else {
            reply.error(Errno::EBADF);
            return;
        };
        let mut buffer = vec![0; size as usize];
        match handle.file.read_at(&mut buffer, offset) {
            Ok(read) => reply.data(&buffer[..read]),
            Err(error) => reply.error(error.into()),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let handles = self.handles.read().unwrap();
        let Some(handle) = handles.get(&fh.0) else {
            reply.error(Errno::EBADF);
            return;
        };
        if !handle.writable {
            reply.error(Errno::EPERM);
            return;
        }
        match handle.file.write_at(data, offset) {
            Ok(written) => {
                drop(handles);
                let mut state = self.state.write().unwrap();
                if let Some(node) = state.nodes.get_mut(&ino.0) {
                    node.size = node.size.max(offset + written as u64);
                }
                reply.written(written as u32);
            }
            Err(error) => reply.error(error.into()),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        if self.handles.read().unwrap().contains_key(&fh.0) {
            reply.ok();
        } else {
            reply.error(Errno::EBADF);
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let handles = self.handles.read().unwrap();
        let Some(handle) = handles.get(&fh.0) else {
            reply.error(Errno::EBADF);
            return;
        };
        let result = if datasync {
            handle.file.sync_data()
        } else {
            handle.file.sync_all()
        };
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.into()),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.handles.write().unwrap().remove(&fh.0);
        self.backings.lock().unwrap().by_handle.remove(&fh.0);
        reply.ok();
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut state = self.state.write().unwrap();
        let Some(ino) = state.child(parent.0, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if matches!(state.nodes[&ino].kind, NodeKind::Directory(_)) {
            reply.error(Errno::EISDIR);
            return;
        }
        let NodeKind::Directory(children) = &mut state.nodes.get_mut(&parent.0).unwrap().kind else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        children.remove(name);
        state.drop_link(ino);
        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut state = self.state.write().unwrap();
        let Some(ino) = state.child(parent.0, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match &state.nodes[&ino].kind {
            NodeKind::Directory(children) if !children.is_empty() => {
                reply.error(Errno::ENOTEMPTY);
                return;
            }
            NodeKind::Directory(_) => {}
            _ => {
                reply.error(Errno::ENOTDIR);
                return;
            }
        }
        let NodeKind::Directory(children) = &mut state.nodes.get_mut(&parent.0).unwrap().kind else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        children.remove(name);
        state.remove_tree(ino);
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if !flags.is_empty() {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        let mut state = self.state.write().unwrap();
        let Some(ino) = state.child(parent.0, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if ino == state.child(newparent.0, newname).unwrap_or(0) {
            reply.ok();
            return;
        }
        if matches!(state.nodes[&ino].kind, NodeKind::Directory(_))
            && state.is_ancestor(ino, newparent.0)
        {
            reply.error(Errno::EINVAL);
            return;
        }
        if !matches!(state.nodes.get(&newparent.0).map(|n| &n.kind), Some(NodeKind::Directory(_))) {
            reply.error(if state.nodes.contains_key(&newparent.0) {
                Errno::ENOTDIR
            } else {
                Errno::ENOENT
            });
            return;
        }
        if let Some(replaced) = state.child(newparent.0, newname) {
            let source_dir = matches!(state.nodes[&ino].kind, NodeKind::Directory(_));
            let target_dir = matches!(state.nodes[&replaced].kind, NodeKind::Directory(_));
            if source_dir != target_dir {
                reply.error(if source_dir { Errno::ENOTDIR } else { Errno::EISDIR });
                return;
            }
            if let NodeKind::Directory(children) = &state.nodes[&replaced].kind {
                if !children.is_empty() {
                    reply.error(Errno::ENOTEMPTY);
                    return;
                }
            }
            if target_dir {
                state.remove_tree(replaced);
            } else {
                state.drop_link(replaced);
            }
        }
        let NodeKind::Directory(old_children) = &mut state.nodes.get_mut(&parent.0).unwrap().kind else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        old_children.remove(name);
        let NodeKind::Directory(new_children) = &mut state.nodes.get_mut(&newparent.0).unwrap().kind else {
            unreachable!();
        };
        new_children.insert(newname.to_os_string(), ino);
        state.nodes.get_mut(&ino).unwrap().parent = newparent.0;
        reply.ok();
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let mut state = self.state.write().unwrap();
        let Some(node) = state.nodes.get(&ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if matches!(node.kind, NodeKind::Directory(_)) {
            reply.error(Errno::EPERM);
            return;
        }
        if !matches!(node.kind, NodeKind::OutputFile { .. }) {
            reply.error(Errno::EPERM);
            return;
        }
        if state.child(newparent.0, newname).is_some() {
            reply.error(Errno::EEXIST);
            return;
        }
        let NodeKind::Directory(children) = &mut state.nodes.get_mut(&newparent.0).unwrap().kind else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        children.insert(newname.to_os_string(), ino.0);
        let node = state.nodes.get_mut(&ino.0).unwrap();
        node.nlink += 1;
        reply.entry(&MUTABLE_TTL, &self.attr(ino.0, node), Generation(0));
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let state = self.state.read().unwrap();
        let Some(node) = state.nodes.get(&ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let NodeKind::Directory(children) = &node.kind else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        let mut entries = Vec::with_capacity(children.len() + 2);
        entries.push((ino.0, FileType::Directory, OsString::from(".")));
        entries.push((node.parent, FileType::Directory, OsString::from("..")));
        entries.extend(children.iter().map(|(name, child)| {
            (*child, state.nodes[child].file_type(), name.clone())
        }));
        for (index, (child, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(child), (index + 1) as u64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn access(&self, _req: &Request, ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        let state = self.state.read().unwrap();
        let Some(node) = state.nodes.get(&ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if mask.contains(AccessFlags::W_OK) && !node.writable() {
            reply.error(Errno::EACCES);
        } else if mask.contains(AccessFlags::X_OK) && node.perm & 0o111 == 0 {
            reply.error(Errno::EACCES);
        } else {
            reply.ok();
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        if mode != 0 {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        match offset.checked_add(length).ok_or(Errno::EFBIG).and_then(|size| self.truncate(ino.0, size, Some(fh))) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let path = std::ffi::CString::new(self.scratch.as_os_str().as_bytes()).unwrap();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
            reply.error(io::Error::last_os_error().into());
            return;
        }
        reply.statfs(
            stat.f_blocks,
            stat.f_bfree,
            stat.f_bavail,
            stat.f_files,
            stat.f_ffree,
            stat.f_bsize as u32,
            stat.f_namemax as u32,
            stat.f_frsize as u32,
        );
    }

    fn setxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _value: &[u8], _flags: i32, _position: u32, reply: ReplyEmpty) {
        reply.error(Errno::EOPNOTSUPP);
    }

    fn getxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::EOPNOTSUPP);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::EOPNOTSUPP);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EOPNOTSUPP);
    }
}

#[derive(Debug)]
struct WireEntry {
    kind: u8,
    path: PathBuf,
    executable: bool,
    size: u64,
    value: OsString,
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut value = [0; 1];
    reader.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut value = [0; 4];
    reader.read_exact(&mut value)?;
    Ok(u32::from_be_bytes(value))
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut value = [0; 8];
    reader.read_exact(&mut value)?;
    Ok(u64::from_be_bytes(value))
}

fn read_os_string(reader: &mut impl Read) -> io::Result<OsString> {
    let length = read_u32(reader)? as usize;
    if length > 16 * 1024 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized protocol string"));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(OsString::from_vec(bytes))
}

fn write_response(writer: &mut impl Write, result: Result<(), String>) -> io::Result<()> {
    let (status, message) = match result {
        Ok(()) => (0u8, Vec::new()),
        Err(message) => (1u8, message.into_bytes()),
    };
    writer.write_all(&[status])?;
    writer.write_all(&(message.len() as u32).to_be_bytes())?;
    writer.write_all(&message)?;
    writer.flush()
}

fn valid_component(name: &OsStr) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.as_bytes().contains(&b'/')
}

fn path_components(path: &OsStr) -> Result<Vec<OsString>, String> {
    let path = Path::new(path);
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(name) if valid_component(name) => Ok(name.to_os_string()),
            _ => Err(format!("invalid input root: {}", path.display())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if components.is_empty() {
        return Err("input root must not be empty".to_string());
    }
    Ok(components)
}

fn add_root(state: &Arc<RwLock<State>>, topdir: OsString, entries: Vec<WireEntry>) -> Result<(), String> {
    let components = path_components(&topdir)?;
    let mut state = state.write().unwrap();
    let mut parent = INodeNo::ROOT.0;
    for component in &components[..components.len() - 1] {
        parent = if let Some(existing) = state.child(parent, component) {
            if !matches!(state.nodes[&existing].kind, NodeKind::Directory(_)) {
                return Err(format!(
                    "input root parent is not a directory: {}",
                    Path::new(&topdir).display()
                ));
            }
            existing
        } else {
            state
                .insert_child(
                    parent,
                    component.clone(),
                    Node {
                        parent,
                        kind: NodeKind::Directory(BTreeMap::new()),
                        perm: 0o755,
                        size: 0,
                        nlink: 2,
                        immutable: false,
                    },
                )
                .map_err(|error| format!("cannot create input root parent: {error:?}"))?
        };
    }
    let root = state
        .insert_child(
            parent,
            components.last().unwrap().clone(),
            Node {
                parent,
                kind: NodeKind::Directory(BTreeMap::new()),
                perm: 0o755,
                size: 0,
                nlink: 2,
                immutable: false,
            },
        )
        .map_err(|error| format!("cannot add root: {error:?}"))?;

    let result = (|| {
        for entry in entries {
            let name = entry
                .path
                .file_name()
                .ok_or_else(|| format!("entry has no name: {}", entry.path.display()))?;
            if !valid_component(name) {
                return Err(format!("invalid entry name: {}", entry.path.display()));
            }
            let parent_path = entry.path.parent().unwrap_or_else(|| Path::new(""));
            let parent = state.resolve_relative(root, parent_path)?;
            let node = match entry.kind {
                ENTRY_DIRECTORY => Node {
                    parent,
                    kind: NodeKind::Directory(BTreeMap::new()),
                    perm: 0o755,
                    size: 0,
                    nlink: 2,
                    immutable: true,
                },
                ENTRY_FILE => Node {
                    parent,
                    kind: NodeKind::InputFile {
                        path: if entry.size == 0 { None } else { Some(PathBuf::from(entry.value)) },
                    },
                    perm: if entry.executable { 0o555 } else { 0o444 },
                    size: entry.size,
                    nlink: 1,
                    immutable: true,
                },
                ENTRY_SYMLINK => Node {
                    parent,
                    kind: NodeKind::Symlink(entry.value.clone()),
                    perm: 0o777,
                    size: entry.value.as_bytes().len() as u64,
                    nlink: 1,
                    immutable: true,
                },
                other => return Err(format!("unknown entry kind {other}")),
            };
            state
                .insert_child(parent, name.to_os_string(), node)
                .map_err(|error| format!("cannot add {}: {error:?}", entry.path.display()))?;
        }
        Ok(())
    })();
    if result.is_err() {
        if let NodeKind::Directory(children) = &mut state.nodes.get_mut(&parent).unwrap().kind {
            children.retain(|_, child| *child != root);
        }
        state.remove_tree(root);
    }
    result
}

fn remove_root(state: &Arc<RwLock<State>>, topdir: &OsStr) -> Result<(), String> {
    let components = path_components(topdir)?;
    let mut state = state.write().unwrap();
    let mut parent = INodeNo::ROOT.0;
    for component in &components[..components.len() - 1] {
        let Some(next) = state.child(parent, component) else {
            return Ok(());
        };
        parent = next;
    }
    let name = components.last().unwrap();
    let Some(ino) = state.child(parent, name) else {
        return Ok(());
    };
    let NodeKind::Directory(children) = &mut state.nodes.get_mut(&parent).unwrap().kind else {
        unreachable!();
    };
    children.remove(name);
    state.remove_tree(ino);
    Ok(())
}

fn serve_control(state: Arc<RwLock<State>>) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = BufWriter::new(stdout.lock());
    write_response(&mut writer, Ok(()))?;
    loop {
        let command = match read_u8(&mut reader) {
            Ok(command) => command,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        match command {
            COMMAND_ADD_ROOT => {
                let version = read_u32(&mut reader)?;
                if version != PROTOCOL_VERSION {
                    write_response(&mut writer, Err(format!("unsupported protocol version {version}")))?;
                    continue;
                }
                let topdir = read_os_string(&mut reader)?;
                let count = read_u32(&mut reader)? as usize;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push(WireEntry {
                        kind: read_u8(&mut reader)?,
                        path: PathBuf::from(read_os_string(&mut reader)?),
                        executable: read_u8(&mut reader)? != 0,
                        size: read_u64(&mut reader)?,
                        value: read_os_string(&mut reader)?,
                    });
                }
                write_response(&mut writer, add_root(&state, topdir, entries))?;
            }
            COMMAND_REMOVE_ROOT => {
                let topdir = read_os_string(&mut reader)?;
                write_response(&mut writer, remove_root(&state, &topdir))?;
            }
            COMMAND_SHUTDOWN => {
                write_response(&mut writer, Ok(()))?;
                return Ok(());
            }
            other => write_response(&mut writer, Err(format!("unknown command {other}")))?,
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args_os();
    let _program = args.next();
    let mount = PathBuf::from(args.next().ok_or("missing mount path")?);
    let scratch = PathBuf::from(args.next().ok_or("missing scratch path")?);
    if args.next().is_some() {
        return Err("usage: buildfarm-fuse MOUNT_PATH SCRATCH_PATH".to_string());
    }
    fs::create_dir_all(&mount).map_err(|error| format!("create mount path: {error}"))?;
    fs::create_dir_all(&scratch).map_err(|error| format!("create scratch path: {error}"))?;
    File::create(scratch.join("empty-input"))
        .map_err(|error| format!("create empty input backing file: {error}"))?;
    let state = Arc::new(RwLock::new(State::new()));
    let filesystem = BuildfarmFuse::new(Arc::clone(&state), scratch);
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("buildfarm".to_string()),
        MountOption::DefaultPermissions,
        MountOption::Exec,
        MountOption::NoAtime,
    ];
    config.acl = SessionACL::Owner;
    config.n_threads = Some(std::thread::available_parallelism().map_or(4, usize::from).min(32));
    config.clone_fd = true;
    let session = fuser::spawn_mount(filesystem, &mount, &config)
        .map_err(|error| format!("mount {}: {error}", mount.display()))?;
    let result = serve_control(state).map_err(|error| format!("control protocol: {error}"));
    drop(session);
    result
}

fn main() {
    if let Err(error) = run() {
        eprintln!("buildfarm-fuse: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_lifecycle_is_atomic() {
        let state = Arc::new(RwLock::new(State::new()));
        add_root(
            &state,
            OsString::from("operation"),
            vec![
                WireEntry {
                    kind: ENTRY_DIRECTORY,
                    path: PathBuf::from("dir"),
                    executable: false,
                    size: 0,
                    value: OsString::new(),
                },
                WireEntry {
                    kind: ENTRY_FILE,
                    path: PathBuf::from("dir/tool"),
                    executable: true,
                    size: 7,
                    value: OsString::from("/cas/tool"),
                },
            ],
        )
        .unwrap();
        let locked = state.read().unwrap();
        let operation = locked.child(INodeNo::ROOT.0, OsStr::new("operation")).unwrap();
        let directory = locked.child(operation, OsStr::new("dir")).unwrap();
        let tool = locked.child(directory, OsStr::new("tool")).unwrap();
        assert_eq!(locked.nodes[&tool].perm, 0o555);
        drop(locked);
        remove_root(&state, OsStr::new("operation")).unwrap();
        assert!(state.read().unwrap().child(INodeNo::ROOT.0, OsStr::new("operation")).is_none());
    }

    #[test]
    fn failed_root_install_rolls_back() {
        let state = Arc::new(RwLock::new(State::new()));
        let result = add_root(
            &state,
            OsString::from("operation"),
            vec![WireEntry {
                kind: ENTRY_FILE,
                path: PathBuf::from("missing/file"),
                executable: false,
                size: 1,
                value: OsString::from("/cas/file"),
            }],
        );
        assert!(result.is_err());
        assert!(state.read().unwrap().child(INodeNo::ROOT.0, OsStr::new("operation")).is_none());
    }
}
