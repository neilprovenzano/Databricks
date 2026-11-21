// Per-thread bump arenas (huge-page advised) and bucketed record stores built from
// fixed-size blocks. Arenas can be reset and reused, so memory faulted in once is
// reused across phases.

use std::marker::PhantomData;

/// Size of every record block; blocks can be recycled between stores.
pub const BLOCK: usize = 64 << 10;

/// Records per block for T (a multiple of 8, the write-combining batch).
pub const fn blk_of<T>() -> usize {
    (BLOCK / std::mem::size_of::<T>()) / 8 * 8
}

pub struct Arena {
    free: Vec<*mut u8>,
    chunks: Vec<(*mut u8, usize)>,
    cur: usize, // index of the chunk in use
    off: usize,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

const ARENA_CHUNK: usize = 64 << 20;

fn map_chunk(cap: usize) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            cap + (2 << 20),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "arena mmap failed");
        let a = ((p as usize) + (2 << 20) - 1) & !((2 << 20) - 1);
        if std::env::var_os("NOHUGE").is_none() {
            libc::madvise(a as *mut libc::c_void, cap, libc::MADV_HUGEPAGE);
        }
        a as *mut u8
    }
}

impl Arena {
    pub fn new(_hint: usize) -> Arena {
        Arena { free: Vec::new(), chunks: Vec::new(), cur: 0, off: 0 }
    }
    /// Hand a dead BLOCK-sized block to this arena for reuse.
    pub fn give(&mut self, p: *mut u8) {
        self.free.push(p);
    }
    #[inline]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        if bytes <= BLOCK {
            if let Some(p) = self.free.pop() {
                return p;
            }
        }
        loop {
            if self.cur < self.chunks.len() {
                let (base, cap) = self.chunks[self.cur];
                let off = (self.off + 63) & !63;
                if off + bytes <= cap {
                    self.off = off + bytes;
                    return unsafe { base.add(off) };
                }
                self.cur += 1;
                self.off = 0;
                continue;
            }
            let cap = ARENA_CHUNK.max((bytes + (2 << 20) - 1) & !((2 << 20) - 1));
            self.chunks.push((map_chunk(cap), cap));
        }
    }
    pub fn reset(&mut self) {
        self.cur = 0;
        self.off = 0;
    }
}

pub struct Head<T> {
    cur: *mut T,
    len: u32,
    blocks: Vec<*mut T>,
}

pub struct Store<T: Copy> {
    heads: Vec<Head<T>>,
    blk: u32,
    _p: PhantomData<T>,
}
unsafe impl<T: Copy + Send> Send for Store<T> {}
unsafe impl<T: Copy + Sync> Sync for Store<T> {}

impl<T: Copy> Store<T> {
    /// The blocks of bucket b (to recycle once the bucket is consumed).
    pub fn blocks(&self, b: usize) -> &[*mut T] {
        &self.heads[b].blocks
    }
    pub fn new(nb: usize, blk: usize) -> Self {
        assert!(blk * std::mem::size_of::<T>() <= BLOCK);
        Store {
            heads: (0..nb).map(|_| Head { cur: std::ptr::null_mut(), len: blk as u32, blocks: Vec::new() }).collect(),
            blk: blk as u32,
            _p: PhantomData,
        }
    }
    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, b: usize, v: T) {
        let h = unsafe { self.heads.get_unchecked_mut(b) };
        if h.len == self.blk {
            let p = arena.alloc(BLOCK) as *mut T;
            h.blocks.push(p);
            h.cur = p;
            h.len = 0;
        }
        unsafe { h.cur.add(h.len as usize).write(v) };
        h.len += 1;
    }
    pub fn len(&self, b: usize) -> usize {
        let h = &self.heads[b];
        if h.blocks.is_empty() {
            0
        } else {
            (h.blocks.len() - 1) * self.blk as usize + h.len as usize
        }
    }
    #[inline]
    pub fn for_each_slice(&self, b: usize, mut f: impl FnMut(&[T])) {
        let h = &self.heads[b];
        let n = h.blocks.len();
        for (i, &p) in h.blocks.iter().enumerate() {
            let len = if i + 1 == n { h.len as usize } else { self.blk as usize };
            f(unsafe { std::slice::from_raw_parts(p, len) });
        }
    }
}

impl<T: Copy> Store<T> {
    /// Reserve k contiguous slots in bucket b (block capacity must be a multiple of k
    /// and the bucket must have only been filled in multiples of k).
    #[inline(always)]
    pub fn reserve(&mut self, arena: &mut Arena, b: usize, k: usize) -> *mut T {
        let h = unsafe { self.heads.get_unchecked_mut(b) };
        if h.len == self.blk {
            let p = arena.alloc(BLOCK) as *mut T;
            h.blocks.push(p);
            h.cur = p;
            h.len = 0;
        }
        let p = unsafe { h.cur.add(h.len as usize) };
        h.len += k as u32;
        p
    }
}

/// Software write-combining scatter: K records per bucket are staged in a small
/// cache-resident buffer and flushed with non-temporal stores.
pub struct Scat<T: Copy, const K: usize> {
    pub store: Store<T>,
    buf: Vec<T>,
    fill: Vec<u32>,
}

impl<T: Copy, const K: usize> Scat<T, K> {
    pub fn new(nb: usize, blk: usize) -> Self {
        assert!(blk % K == 0);
        assert!((K * std::mem::size_of::<T>()) % 32 == 0);
        let mut buf = Vec::with_capacity(nb * K);
        unsafe { buf.set_len(nb * K) };
        Scat { store: Store::new(nb, blk), buf, fill: vec![0; nb] }
    }
    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, b: usize, v: T) {
        unsafe {
            let f = *self.fill.get_unchecked(b) as usize;
            *self.buf.get_unchecked_mut(b * K + f) = v;
            if f + 1 == K {
                *self.fill.get_unchecked_mut(b) = 0;
                let dst = self.store.reserve(arena, b, K) as *mut u8;
                let src = self.buf.as_ptr().add(b * K) as *const u8;
                stream_copy(dst, src, K * std::mem::size_of::<T>());
            } else {
                *self.fill.get_unchecked_mut(b) = (f + 1) as u32;
            }
        }
    }
    pub fn finish(mut self, arena: &mut Arena) -> Store<T> {
        for b in 0..self.fill.len() {
            let f = self.fill[b] as usize;
            for i in 0..f {
                let v = self.buf[b * K + i];
                self.store.push(arena, b, v);
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
        self.store
    }
}

#[inline(always)]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, n: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let mut i = 0;
        while i < n {
            let v = _mm256_loadu_si256(src.add(i) as *const __m256i);
            _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
            i += 32;
        }
        return;
    }
    #[allow(unreachable_code)]
    std::ptr::copy_nonoverlapping(src, dst, n);
}

/// Global allocator: large allocations (>= 1 MB) get their own huge-page-advised
/// mapping, so big tables and buffers fault in 2 MB at a time; the rest go to malloc.
pub struct HugeAlloc;

const HUGE_MIN: usize = 1 << 20;

#[inline]
fn round_up(n: usize) -> usize {
    (n + 4095) & !4095
}

unsafe fn big_map(size: usize) -> *mut u8 {
    let n = round_up(size);
    let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
    if p == libc::MAP_FAILED {
        return std::ptr::null_mut();
    }
    if n >= (2 << 20) {
        libc::madvise(p, n, libc::MADV_HUGEPAGE);
    }
    p as *mut u8
}

unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= HUGE_MIN && l.align() <= 4096 {
            big_map(l.size())
        } else {
            std::alloc::System.alloc(l)
        }
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= HUGE_MIN && l.align() <= 4096 {
            big_map(l.size()) // fresh anonymous memory is zero
        } else {
            std::alloc::System.alloc_zeroed(l)
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        if l.size() >= HUGE_MIN && l.align() <= 4096 {
            libc::munmap(p as *mut libc::c_void, round_up(l.size()));
        } else {
            std::alloc::System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, new: usize) -> *mut u8 {
        let old_big = l.size() >= HUGE_MIN && l.align() <= 4096;
        let new_big = new >= HUGE_MIN && l.align() <= 4096;
        if !old_big && !new_big {
            return std::alloc::System.realloc(p, l, new);
        }
        if old_big && new_big {
            let q = libc::mremap(p as *mut libc::c_void, round_up(l.size()), round_up(new), libc::MREMAP_MAYMOVE);
            if q == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            if round_up(new) >= (2 << 20) {
                libc::madvise(q, round_up(new), libc::MADV_HUGEPAGE);
            }
            return q as *mut u8;
        }
        let nl = std::alloc::Layout::from_size_align_unchecked(new, l.align());
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
            self.dealloc(p, l);
        }
        q
    }
}
