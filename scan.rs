static SWAR_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
#[cfg(target_arch = "x86_64")]
static AVX2_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
// Structural CSV scanner. For each line of d[st..en] with exactly N fields and
// valid quoting, calls f(fields) where each field is the (start, end) byte range
// of its value (quotes stripped). Malformed lines are skipped.
//
// Quoting: a line is valid iff every field either holds no quote, or is at
// least 2 bytes long, starts and ends with a quote, and holds exactly 2 quotes.
// Equivalently: the line's quote count equals 2 x (number of fields of length
// >= 2 that start and end with a quote), since any other quote breaks equality.
//
// Per segment: (1) flatten comma positions and newline positions into two arrays
// and keep each 64-byte block's quote mask with a running quote count;
// (2) walk lines: a line is the commas between two newlines.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[inline(always)]
fn masks_scalar(b: &[u8; 64]) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for (i, &x) in b.iter().enumerate() {
        c |= ((x == b',') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
        q |= ((x == b'"') as u64) << i;
    }
    (c, n, q)
}

// Experimental SWAR delimiter detector: eight bytes per 64-bit word.
// SCAN_SWAR=1 enables it on macOS/ARM and other non-AVX2 targets.
// This computes exact per-byte equality masks (no cross-byte borrow errors).
#[inline(always)]
fn eq8(x: u64, byte: u8) -> u8 {
    let z = x ^ (0x0101_0101_0101_0101u64 * byte as u64);
    let low = 0x7f7f_7f7f_7f7f_7f7fu64;
    let high = !( ((z & low).wrapping_add(low)) | z | low ) & 0x8080_8080_8080_8080u64;
    (((high >> 7).wrapping_mul(0x0102_0408_1020_4080u64)) >> 56) as u8
}

#[inline(always)]
fn masks_swar(d: &[u8], base: usize) -> (u64, u64, u64) {
    let mut c = 0u64;
    let mut n = 0u64;
    let mut q = 0u64;
    for i in 0..8 {
        // The caller ensures the 64-byte region is in bounds.
        let x = u64::from_le(unsafe { (d.as_ptr().add(base + i * 8) as *const u64).read_unaligned() });
        let shift = i * 8;
        c |= (eq8(x, b',') as u64) << shift;
        n |= (eq8(x, b'\n') as u64) << shift;
        q |= (eq8(x, b'"') as u64) << shift;
    }
    (c, n, q)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn masks_simd(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |x: __m256i, c: u8| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, _mm256_set1_epi8(c as i8))) as u32 };
    let c = m(a, b',') as u64 | ((m(b, b',') as u64) << 32);
    let n = m(a, b'\n') as u64 | ((m(b, b'\n') as u64) << 32);
    let q = m(a, b'"') as u64 | ((m(b, b'"') as u64) << 32);
    (c, n, q)
}

#[inline(always)]
fn masks(d: &[u8], base: usize) -> (u64, u64, u64) {
    if base + 64 <= d.len() {
        #[cfg(target_arch = "x86_64")]
        {
            // Runtime dispatch works even when the binary was built for generic x86-64.
            // SCAN_AVX2=0 allows a controlled comparison against SWAR/scalar.
            if *AVX2_ENABLED.get_or_init(|| {
                std::env::var_os("SCAN_AVX2").as_deref() != Some(std::ffi::OsStr::new("0"))
                    && std::is_x86_feature_detected!("avx2")
            }) {
                return unsafe { masks_simd(d.as_ptr().add(base)) };
            }
        }
        #[allow(unreachable_code)]
        {
            if *SWAR_ENABLED.get_or_init(|| std::env::var_os("SCAN_SWAR").as_deref() == Some(std::ffi::OsStr::new("1"))) {
                return masks_swar(d, base);
            }
            let mut buf = [0u8; 64];
            buf.copy_from_slice(&d[base..base + 64]);
            masks_scalar(&buf)
        }
    } else {
        let mut buf = [0u8; 64];
        let n = d.len() - base;
        buf[..n].copy_from_slice(&d[base..]);
        masks_scalar(&buf)
    }
}

const SEG: usize = 1 << 15; // bytes per segment (multiple of 64)
const NBLK: usize = SEG / 64;

#[inline(always)]
fn emit<const N: usize>(d: &[u8], ls: usize, le: usize, commas: &[u32], cbase: usize, nq: u32, f: &mut impl FnMut(&[(usize, usize); N])) {
    let mut fl = [(0usize, 0usize); N];
    let mut s = ls;
    for i in 0..N {
        let e = if i + 1 < N { cbase + commas[i] as usize } else { le };
        fl[i] = (s, e);
        s = e + 1;
    }
    let mut quoted = 0u32;
    for fld in fl.iter_mut() {
        let (s, e) = *fld;
        let last = unsafe { *d.get_unchecked(e.max(1) - 1) };
        let first = unsafe { *d.get_unchecked(s.min(d.len() - 1)) };
        let isq = ((e - s >= 2) & (first == b'"') & (last == b'"')) as usize;
        quoted += 2 * isq as u32;
        *fld = (s + isq, e - isq);
    }
    if quoted == nq {
        f(&fl);
    }
}

pub struct Scanner {
    commas: Vec<u32>,
    nls: Vec<u32>,
    qmask: Vec<u64>,
    qpre: Vec<u32>,
}

impl Default for Scanner {
    fn default() -> Self {
        Scanner { commas: vec![0; SEG + 128], nls: vec![0; SEG + 128], qmask: vec![0; NBLK + 1], qpre: vec![0; NBLK + 1] }
    }
}

thread_local! {
    static SCANNER: std::cell::RefCell<Scanner> = std::cell::RefCell::new(Scanner::default());
}

#[inline(always)]
pub fn scan<const N: usize>(d: &[u8], st: usize, en: usize, f: impl FnMut(&[(usize, usize); N])) {
    SCANNER.with(|s| s.borrow_mut().run::<N>(d, st, en, f))
}

impl Scanner {
    #[inline(always)]
    pub fn run<const N: usize>(&mut self, d: &[u8], st: usize, en: usize, mut f: impl FnMut(&[(usize, usize); N])) {
        if st >= en {
            return;
        }
        // Line state carried across segments: start, quotes so far, commas so far.
        let mut ls = st;
        let mut lq = 0u32; // quotes in the current line from earlier segments
        let mut lc = 0usize; // commas in the current line from earlier segments
        let mut carry = [0u32; 16]; // absolute (rel st) comma positions of the current line, first 16
        let mut base = st;
        let commas = &mut self.commas;
        let nls = &mut self.nls;
        let qmask = &mut self.qmask;
        let qpre = &mut self.qpre;
        while base < en {
            let seg_base = base;
            let seg_end = (base + SEG).min(en);
            // ---- stage 1
            let mut nc = 0usize;
            let mut nn = 0usize;
            let mut qacc = 0u32;
            let mut blk = 0usize;
            while base < seg_end {
                let (c, nl, q) = masks(d, base);
                let valid = if en - base >= 64 { !0u64 } else { (1u64 << (en - base)) - 1 };
                let c = c & valid;
                let nl = nl & valid;
                let q = q & valid;
                qmask[blk] = q;
                qpre[blk] = qacc;
                qacc += q.count_ones();
                let rel = (base - seg_base) as u32;
                // commas, 8 at a time
                let cnt = c.count_ones() as usize;
                let mut m = c;
                let mut j = 0;
                loop {
                    for _ in 0..8 {
                        unsafe { *commas.get_unchecked_mut(nc + j) = rel.wrapping_add(m.trailing_zeros()) };
                        m &= m.wrapping_sub(1);
                        j += 1;
                    }
                    if j >= cnt {
                        break;
                    }
                }
                nc += cnt;
                // newlines, 2 at a time
                let cnt = nl.count_ones() as usize;
                let mut m = nl;
                let mut j = 0;
                loop {
                    for _ in 0..2 {
                        unsafe { *nls.get_unchecked_mut(nn + j) = rel.wrapping_add(m.trailing_zeros()) };
                        m &= m.wrapping_sub(1);
                        j += 1;
                    }
                    if j >= cnt {
                        break;
                    }
                }
                nn += cnt;
                base += 64;
                blk += 1;
            }
            qpre[blk] = qacc;
            // quote count before segment-relative position x
            let qbefore = |x: usize| -> u32 {
                let b = x >> 6;
                let r = x & 63;
                qpre[b] + (qmask[b] & ((1u64 << r) - 1)).count_ones()
            };
            // sentinel so the "next comma is after the newline" check needs no bounds test
            commas[nc] = u32::MAX;
            // ---- stage 2
            let mut cp = 0usize; // first comma of the current line in this segment
            let mut prev_q = 0u32; // quotes before the current line's start within this segment
            for k in 0..nn {
                let le_rel = nls[k];
                let le = seg_base + le_rel as usize;
                let qend = qbefore(le_rel as usize);
                let nq = lq + qend - prev_q;
                // commas of this line in this segment: cp .. first comma > le_rel
                if lc == 0 {
                    // common case: the line started in this segment
                    let ok = cp + N - 1 <= nc && (N == 1 || commas[cp + N - 2] < le_rel) && commas[cp + N - 1] > le_rel;
                    if ok {
                        emit::<N>(d, ls, le, &commas[cp..cp + N - 1], seg_base, nq, &mut f);
                        cp += N - 1;
                    } else {
                        while commas[cp] < le_rel {
                            cp += 1;
                        }
                    }
                } else {
                    // line continued from an earlier segment
                    let mut k2 = lc;
                    while commas[cp] < le_rel {
                        if k2 < 16 {
                            carry[k2] = (seg_base - st) as u32 + commas[cp];
                        }
                        k2 += 1;
                        cp += 1;
                    }
                    if k2 == N - 1 {
                        emit::<N>(d, ls, le, &carry[..N - 1], st, nq, &mut f);
                    }
                    lc = 0;
                }
                ls = le + 1;
                lq = 0;
                prev_q = qend;
            }
            // carry the unfinished line (if any) into the next segment
            let seg_len = seg_end - seg_base;
            let tail_q = qbefore(seg_len) - prev_q;
            lq += tail_q;
            while cp < nc {
                if lc < 16 {
                    carry[lc] = (seg_base - st) as u32 + commas[cp];
                }
                lc += 1;
                cp += 1;
            }
        }
        // last line without a trailing newline
        if ls < en || lc > 0 {
            if lc == N - 1 {
                emit::<N>(d, ls, en, &carry[..N - 1], st, lq, &mut f);
            }
        }
    }
}

