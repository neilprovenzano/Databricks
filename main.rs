// Shopping Sessions pipeline, v3.
//
//   P1  parse links (scatter ID sides by user-hash bucket), assign IDs per bucket
//   P2  union-find + naming on one thread  ||  parse events (scatter by event_id bucket)
//                                              and clicks (scatter by user bucket)
//   P3  dedup each event_id bucket -> winners scattered by user bucket
//   P4  per user bucket: user -> person (bucket-local table), scatter by person bucket
//   P5  per person bucket: sort, sessionize, attribute, aggregate
//   P6  write Parquet
//
// A "loc" is 40 bits: file index (9 bits) << 31 | byte offset of a field value
// (31 bits). The value runs to the next ',', '"', '\n' or end of file.

use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use xxhash_rust::xxh3::xxh3_64;

mod mem;
mod out;
mod scan;
use mem::{Arena, Scat, Store};
use scan::scan;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
fn chunk_size() -> usize {
    std::env::var("CHUNK_KB").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(512) * 1024
}

const EB_BITS: u32 = 10; // event_id buckets
const UB_BITS: u32 = 9; // user buckets (also link-ID buckets)
const PB_BITS: u32 = 9; // person buckets
const NEB: usize = 1 << EB_BITS;
const NUB: usize = 1 << UB_BITS;
const NPB: usize = 1 << PB_BITS;
const EID_BITS: u32 = 64 - EB_BITS; // eid bits stored in a record
const EID_MASK: u64 = (1u64 << EID_BITS) - 1;
const LOC_MASK: u64 = (1u64 << 40) - 1;
const TS_INVALID: u32 = u32::MAX;

#[inline(always)]
fn eb(h: u64) -> usize {
    (h >> EID_BITS) as usize
}
#[inline(always)]
fn ub(h: u64) -> usize {
    (h >> (64 - UB_BITS)) as usize
}

// ---------------------------------------------------------------- files

pub struct Files {
    maps: Vec<Option<Mmap>>,
    handles: Vec<Option<File>>,
}

thread_local! {
    static BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::new());
}

impl Files {
    /// Run f on the bytes of file fi in [st, en), read into a reused per-thread buffer
    /// (or straight from the mapping if READ_MMAP is set). f gets (bytes, base offset).
    #[inline]
    fn with_chunk<R>(&self, fi: usize, st: usize, en: usize, use_mmap: bool, f: impl FnOnce(&[u8], usize) -> R) -> R {
        let _ = use_mmap;
        BUF.with(|b| {
            let mut b = b.borrow_mut();
            let n = en - st;
            if b.len() < n + 64 {
                b.resize(n + 64, 0);
            }
            use std::os::unix::fs::FileExt;
            self.handles[fi].as_ref().unwrap().read_exact_at(&mut b[..n], st as u64).expect("read");
            f(&b[..n], st)
        })
    }
}

impl Files {
    /// The field value at loc.
    #[inline]
    pub fn field(&self, loc: u64) -> &[u8] {
        let f = (loc >> 31) as usize & 0x1ff;
        let off = (loc & 0x7fff_ffff) as usize;
        let d = &self.maps[f].as_ref().unwrap()[off..];
        let n = d.iter().position(|&c| c == b',' || c == b'"' || c == b'\n').unwrap_or(d.len());
        &d[..n]
    }
}

#[inline(always)]
fn mkloc(file: usize, off: usize) -> u64 {
    ((file as u64) << 31) | off as u64
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = if std::env::var_os("POPULATE").is_some() {
        unsafe { memmap2::MmapOptions::new().populate().map(&f).ok()? }
    } else {
        unsafe { Mmap::map(&f).ok()? }
    };
    let _ = m.advise(memmap2::Advice::Sequential);
    Some(m)
}

#[inline(always)]
fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&x| x == c)
}

// ---------------------------------------------------------------- time

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a as i64) * 10 + b as i64)
    } else {
        None
    }
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn dim(y: i64, m: i64) -> i64 {
    const D: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) {
        29
    } else {
        D[m as usize]
    }
}

/// Reference ts parser (also the slow path of the fast one).
fn parse_ts_slow(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if s[4] == b'-' && s[7] == b'-' && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            let y = d2(s, 0)? * 100 + d2(s, 2)?;
            let mo = d2(s, 5)?;
            let d = d2(s, 8)?;
            let h = d2(s, 11)?;
            let mi = d2(s, 14)?;
            let se = d2(s, 17)?;
            if !(1..=12).contains(&mo) || d < 1 || d > dim(y, mo) || h > 23 || mi > 59 || se > 59 {
                return None;
            }
            let mut off = 0i64;
            if n == 20 {
                if s[19] != b'Z' {
                    return None;
                }
            } else {
                let sign = match s[19] {
                    b'+' => 1,
                    b'-' => -1,
                    _ => return None,
                };
                if s[22] != b':' {
                    return None;
                }
                let oh = d2(s, 20)?;
                let om = d2(s, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                off = sign * (oh * 3600 + om * 60);
            }
            let days = days_from_civil(y, mo, d);
            let secs = days * 86400 + h * 3600 + mi * 60 + se - off;
            return Some(secs * 1000);
        }
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

#[inline(always)]
fn rd64(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

#[inline(always)]
fn swar_ok(w: u64, digmask: u64, sepmask: u64, sepval: u64) -> bool {
    let t = (w ^ 0x3030_3030_3030_3030) & digmask;
    ((w & sepmask) == sepval) & ((t & 0xF0F0_F0F0_F0F0_F0F0) == 0) & (((t + 0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0)
}

#[inline(always)]
fn byte(w: u64, i: u32) -> i64 {
    ((w >> (8 * i)) & 0xff) as i64 - 0x30
}

/// Days-since-epoch of the 1st and length of months near the window.
pub struct Months {
    base: i64, // y*12 + (m-1) of entry 0
    start: [i64; 8],
    len: [i64; 8],
}

impl Months {
    fn new(ws: i64) -> Self {
        // entry 0 = two months before the window start's month (clicks reach 7 days back)
        let days = ws.div_euclid(DAY_MS);
        // civil from days (Howard Hinnant)
        let z = days + 719468;
        let era = z.div_euclid(146097);
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        let base = y * 12 + (m - 1) - 2;
        let mut start = [0i64; 8];
        let mut len = [0i64; 8];
        for i in 0..8 {
            let k = base + i as i64;
            let (yy, mm) = (k.div_euclid(12), k.rem_euclid(12) + 1);
            start[i] = days_from_civil(yy, mm, 1);
            len[i] = dim(yy, mm);
        }
        Months { base, start, len }
    }
    /// (days of the 1st, month length)
    #[inline(always)]
    fn get(&self, y: i64, mo: i64) -> (i64, i64) {
        let k = (y * 12 + mo - 1 - self.base) as u64;
        if k < 8 {
            (self.start[k as usize], self.len[k as usize])
        } else {
            (days_from_civil(y, mo, 1), dim(y, mo))
        }
    }
}

/// F1 or F2 body (n is 20 or 25); None = bad or not of that shape. SSE version.
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
#[inline(always)]
fn parse_f12(s: &[u8], n: usize, mt: &Months) -> Option<i64> {
    use std::arch::x86_64::*;
    debug_assert!(s.len() == n && n >= 20);
    unsafe {
        let a = _mm_loadu_si128(s.as_ptr() as *const __m128i); // bytes 0..16: YYYY-MM-DDTHH:MM
        let t = _mm_sub_epi8(a, _mm_set1_epi8(b'0' as i8));
        // digits: t <= 9 unsigned; separators must match the template
        let dig_ok = _mm_cmpeq_epi8(_mm_max_epu8(t, _mm_set1_epi8(9)), _mm_set1_epi8(9));
        let tmpl = _mm_setr_epi8(0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0);
        let sep_ok = _mm_cmpeq_epi8(a, tmpl);
        // separator positions 4, 7, 10, 13
        let ok = _mm_movemask_epi8(dig_ok) as u32 & !0x2490u32 | (_mm_movemask_epi8(sep_ok) as u32 & 0x2490u32);
        if ok != 0xFFFF {
            return None;
        }
        // pairs: YY YY MM DD hh mm
        let g = _mm_shuffle_epi8(t, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
        let p = _mm_maddubs_epi16(g, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0));
        let y = (_mm_extract_epi16(p, 0) as i64) * 100 + _mm_extract_epi16(p, 1) as i64;
        let mo = _mm_extract_epi16(p, 2) as i64;
        let d = _mm_extract_epi16(p, 3) as i64;
        let h = _mm_extract_epi16(p, 4) as i64;
        let mi = _mm_extract_epi16(p, 5) as i64;
        let s16 = *s.get_unchecked(16);
        let s0 = (*s.get_unchecked(17)).wrapping_sub(b'0');
        let s1 = (*s.get_unchecked(18)).wrapping_sub(b'0');
        let ok = (s16 == b':') & (s0 < 6) & (s1 < 10) & (mo >= 1) & (mo <= 12) & (d >= 1) & (h <= 23) & (mi <= 59);
        if !ok {
            return None;
        }
        let se = (s0 as i64) * 10 + s1 as i64;
        let (m1, ml) = mt.get(y, mo);
        if d > ml {
            return None;
        }
        let z = *s.get_unchecked(19);
        let off = if n == 20 {
            if z != b'Z' {
                return None;
            }
            0
        } else {
            let sign = match z {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 3600 + om * 60)
        };
        Some(((m1 + d - 1) * 86400 + h * 3600 + mi * 60 + se - off) * 1000)
    }
}

/// F1 or F2 body (n is 20 or 25); None = bad or not of that shape.
#[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
#[inline(always)]
fn parse_f12(s: &[u8], n: usize, mt: &Months) -> Option<i64> {
    let w0 = rd64(s, 0);
    let w1 = rd64(s, 8);
    let w2 = rd64(s, 11);
    const D0: u64 = 0x00FF_FF00_FFFF_FFFF;
    const S0: u64 = 0xFF00_00FF_0000_0000;
    const V0: u64 = 0x2D00_002D_0000_0000;
    const D1: u64 = 0xFFFF_00FF_FF00_FFFF;
    const S1: u64 = 0x0000_FF00_00FF_0000;
    const V1: u64 = 0x0000_3A00_0054_0000;
    const V2: u64 = 0x0000_3A00_003A_0000;
    if !(swar_ok(w0, D0, S0, V0) & swar_ok(w1, D1, S1, V1) & swar_ok(w2, D1, S1, V2)) {
        return None;
    }
    let y = byte(w0, 0) * 1000 + byte(w0, 1) * 100 + byte(w0, 2) * 10 + byte(w0, 3);
    let mo = byte(w0, 5) * 10 + byte(w0, 6);
    let d = byte(w1, 0) * 10 + byte(w1, 1);
    let h = byte(w1, 3) * 10 + byte(w1, 4);
    let mi = byte(w1, 6) * 10 + byte(w1, 7);
    let se = byte(w2, 6) * 10 + byte(w2, 7);
    let ok = (mo >= 1) & (mo <= 12) & (d >= 1) & (h <= 23) & (mi <= 59) & (se <= 59);
    if !ok {
        return None;
    }
    let (m1, ml) = mt.get(y, mo);
    if d > ml {
        return None;
    }
    let mut off = 0i64;
    if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = sign * (oh * 3600 + om * 60);
    }
    Some(((m1 + d - 1) * 86400 + h * 3600 + mi * 60 + se - off) * 1000)
}

/// Fast path for a value that is almost always F1 (ingest_ts).
#[inline(always)]
fn parse_ts_f1(s: &[u8], mt: &Months) -> Option<i64> {
    if s.len() == 20 {
        if let Some(v) = parse_f12(s, 20, mt) {
            return Some(v);
        }
    }
    parse_ts(s)
}

/// Digits-only value of 1..=16 bytes; None if any byte is not an ASCII digit.
/// Reads 16 bytes starting at s[0]: the caller guarantees they are readable.
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
#[inline(always)]
unsafe fn digits16(p: *const u8, n: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    static SHUF: [[i8; 16]; 17] = {
        let mut t = [[-1i8; 16]; 17];
        let mut n = 0;
        while n <= 16 {
            // right-align n digits: output byte 16-n+i = input byte i
            let mut i = 0;
            while i < n {
                t[n][16 - n + i] = i as i8;
                i += 1;
            }
            n += 1;
        }
        t
    };
    let v = _mm_loadu_si128(p as *const __m128i);
    let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    // only the first n bytes matter
    let badm = !(_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_max_epu8(t, _mm_set1_epi8(9)), _mm_set1_epi8(9))) as u32) & ((1u32 << n) - 1);
    if badm != 0 {
        return None;
    }
    let r = _mm_shuffle_epi8(t, _mm_loadu_si128(SHUF[n].as_ptr() as *const __m128i)); // pads with 0
    let p2 = _mm_maddubs_epi16(r, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1)); // 8 x 2 digits
    let p4 = _mm_madd_epi16(p2, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1)); // 4 x 4 digits
    let p4 = _mm_packus_epi32(p4, p4); // u16 x 4 (each < 10000)
    let p8 = _mm_madd_epi16(p4, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1)); // 2 x 8 digits
    let hi = _mm_cvtsi128_si32(p8) as u32 as u64;
    let lo = _mm_extract_epi32(p8, 1) as u32 as u64;
    Some(hi * 100_000_000 + lo)
}

/// Parse a ts with the month table.
#[inline(always)]
fn parse_ts_m(s: &[u8], mt: &Months) -> Option<i64> {
    let n = s.len();
    if n == 20 || n == 25 {
        if let Some(v) = parse_f12(s, n, mt) {
            return Some(v);
        }
        return parse_ts_slow(s);
    }
    parse_ts(s)
}

/// As parse_ts_m, but may read up to 16 bytes from s.as_ptr() (the buffer must allow it).
#[inline(always)]
unsafe fn parse_ts_m16(s: &[u8], mt: &Months) -> Option<i64> {
    let n = s.len();
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
    {
        if n >= 1 && n <= 16 {
            return digits16(s.as_ptr(), n).map(|v| v as i64);
        }
    }
    parse_ts_m(s, mt)
}

/// Parse a ts into epoch ms. None = bad (including overflow).
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 || n == 25 {
        let w0 = rd64(s, 0);
        let w1 = rd64(s, 8);
        let w2 = rd64(s, 11);
        const D0: u64 = 0x00FF_FF00_FFFF_FFFF;
        const S0: u64 = 0xFF00_00FF_0000_0000;
        const V0: u64 = 0x2D00_002D_0000_0000;
        const D1: u64 = 0xFFFF_00FF_FF00_FFFF;
        const S1: u64 = 0x0000_FF00_00FF_0000;
        const V1: u64 = 0x0000_3A00_0054_0000;
        const V2: u64 = 0x0000_3A00_003A_0000;
        if !(swar_ok(w0, D0, S0, V0) & swar_ok(w1, D1, S1, V1) & swar_ok(w2, D1, S1, V2)) {
            return parse_ts_slow(s);
        }
        let y = byte(w0, 0) * 1000 + byte(w0, 1) * 100 + byte(w0, 2) * 10 + byte(w0, 3);
        let mo = byte(w0, 5) * 10 + byte(w0, 6);
        let d = byte(w1, 0) * 10 + byte(w1, 1);
        let h = byte(w1, 3) * 10 + byte(w1, 4);
        let mi = byte(w1, 6) * 10 + byte(w1, 7);
        let se = byte(w2, 6) * 10 + byte(w2, 7);
        let ok = (mo >= 1) & (mo <= 12) & (d >= 1) & (h <= 23) & (mi <= 59) & (se <= 59);
        if !ok || d > dim(y, mo) {
            return None;
        }
        let mut off = 0i64;
        if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
        } else {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            off = sign * (oh * 3600 + om * 60);
        }
        let days = days_from_civil(y, mo, d);
        return Some((days * 86400 + h * 3600 + mi * 60 + se - off) * 1000);
    }
    if n == 0 {
        return None;
    }
    if n <= 18 {
        let mut v: u64 = 0;
        let mut bad = false;
        for &c in s {
            let dd = c.wrapping_sub(b'0');
            bad |= dd >= 10;
            v = v.wrapping_mul(10).wrapping_add(dd as u64);
        }
        return if bad { None } else { Some(v as i64) };
    }
    parse_ts_slow(s)
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut bad = false;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        bad |= d >= 10;
        v = (v * 10 + d as u64).min(10_000_001);
    }
    if !bad && (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

/// 64-bit hash of a short string; never 0.
#[inline(always)]
fn h64(s: &[u8]) -> u64 {
    let n = s.len();
    let (a, b) = if n >= 8 {
        if n > 16 {
            let h = xxh3_64(s);
            return if h == 0 { 1 } else { h };
        }
        (rd64(s, 0), rd64(s, n - 8))
    } else if n >= 4 {
        let lo = u32::from_le_bytes(s[0..4].try_into().unwrap()) as u64;
        let hi = u32::from_le_bytes(s[n - 4..n].try_into().unwrap()) as u64;
        (lo | (hi << 32), 0)
    } else if n > 0 {
        ((s[0] as u64) | ((s[n / 2] as u64) << 8) | ((s[n - 1] as u64) << 16), 0)
    } else {
        (0, 0)
    };
    let h = mum(a ^ 0x2d35_8dcc_aa6c_78a5, b ^ 0x8bb8_4b93_962e_acc9 ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let h = mum(h ^ 0x4b33_a62e_d433_d4a3, 0x4d5a_2da5_1de1_aa47 ^ n as u64);
    if h == 0 {
        1
    } else {
        h
    }
}

// ---------------------------------------------------------------- hash map

/// Open-addressing u64 -> V map, keys nonzero. Slot from the low bits.
pub struct U64Map<V: Copy + Default> {
    keys: Vec<u64>,
    vals: Vec<V>,
    mask: usize,
    len: usize,
}

impl<V: Copy + Default> U64Map<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        U64Map { keys: vec![0; cap], vals: vec![V::default(); cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    pub fn entry(&mut self, k: u64) -> (usize, bool) {
        if self.len * 2 >= self.keys.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return (i, true);
            }
            if kk == 0 {
                self.keys[i] = k;
                self.len += 1;
                return (i, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<&V> {
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old_k = std::mem::take(&mut self.keys);
        let old_v = std::mem::take(&mut self.vals);
        let cap = old_k.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![V::default(); cap];
        self.mask = cap - 1;
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != 0 {
                let mut i = (k as usize) & self.mask;
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }
    #[inline(always)]
    pub fn val_mut(&mut self, i: usize) -> &mut V {
        &mut self.vals[i]
    }
}

// ---------------------------------------------------------------- records

/// Event copy, scattered by event_id bucket.
///   a: eid low EID_BITS | cat << 56 (bucket bits are implied)
///   user: user hash
///   b: user loc (40) | price << 40 (0 unless purchase)
///   c: ts rel window start in ms (u32, TS_INVALID if invalid) | ingest secs (i32) << 32
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct ERec {
    a: u64,
    user: u64,
    b: u64,
    c: u64,
}

/// Surviving event: user hash, loc|price<<40, ts | cat<<32.
#[derive(Clone, Copy)]
#[repr(C)]
struct WRec {
    user: u64,
    b: u64,
    c: u64,
}

/// Person event: person key, name loc|price<<40, ts | cat<<32.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PRec {
    pkey: u64,
    b: u64,
    c: u64,
}

/// Click: key (user hash, then person key), click_id loc | camp << 40, ts rel window start.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    key: u64,
    b: u64,
    ts: i64,
}


pub struct UserRow {
    pub nloc: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top: u8, // 255 = null
}

#[derive(Clone, Copy, Default)]
pub struct CampAgg {
    pub sessions: u64,
    pub purch: u64,
    pub revenue: u64,
    pub assisted: u64,
}

struct Ctx {
    ws: i64,
    we: i64,
    prod: U64Map<u8>,
    unknown_cat: u8,
    camps: U64Map<u16>,
    months: Months,
    nopush: bool,

}

// ---------------------------------------------------------------- small csv

fn read_small(p: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(p).unwrap_or_default();
    let mut v = Vec::new();
    let hdr_end = memchr(b'\n', &data).map(|i| i + 1).unwrap_or(data.len());
    scan::<2>(&data, hdr_end, data.len(), |f| {
        v.push((data[f[0].0..f[0].1].to_vec(), data[f[1].0..f[1].1].to_vec()));
    });
    v
}

fn parse_manifest_ts(s: &str, key: &str) -> i64 {
    let i = s.find(key).expect("manifest key");
    let rest = &s[i + key.len()..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

// ---------------------------------------------------------------- work

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Event,
    Click,
    Link,
}

fn chunks(files: &Files, ids: &[usize], kind: Kind, out: &mut Vec<(Kind, usize, usize, usize)>) {
    let chunk = chunk_size();
    for &fi in ids {
        let Some(m) = files.maps[fi].as_ref() else { continue };
        let n = m.len();
        let mut s = memchr(b'\n', m).map(|i| i + 1).unwrap_or(n);
        while s < n {
            let mut e = (s + chunk).min(n);
            if e < n {
                e = match memchr(b'\n', &m[e..]) {
                    Some(i) => e + i + 1,
                    None => n,
                };
            }
            out.push((kind, fi, s, e));
            s = e;
        }
    }
}

/// Run `f(thread_index)` on n threads and collect results.
fn par<R: Send>(n: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (0..n).map(|t| s.spawn(move || f(t))).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Per user bucket: map user hash -> local id; and per local id the person (name hash, name loc).
struct LinkBucket {
    map: U64Map<u32>,
    hashes: Vec<u64>,
    locs: Vec<u64>,
    person: Vec<(u64, u64)>,
}

fn microbench(path: &str) {
    // parse a 512 KB slice repeatedly (cache-resident, as in the pipeline)
    let full = std::fs::read(path).unwrap();
    let st = memchr(b'\n', &full).unwrap() + 1;
    let mut en = st + (512 << 10);
    while full[en - 1] != b'\n' {
        en += 1;
    }
    let mut d = full[st..en].to_vec();
    d.extend_from_slice(&[0u8; 64]);
    let len = en - st;
    let mt = Months::new(1_796_000_000_000);
    let mut lines = 0usize;
    scan::<7>(&d, 0, len, |_| lines += 1);
    let reps = 200;
    for mode in 0..8 {
        let t = std::time::Instant::now();
        let mut acc = 0u64;
        for _ in 0..reps {
            scan::<7>(&d, 0, len, |f| {
                acc = acc.wrapping_add(f[6].1 as u64);
                if mode >= 1 { acc = acc.wrapping_add(h64(&d[f[0].0..f[0].1])); }
                if mode >= 2 { acc = acc.wrapping_add(parse_ts_f1(&d[f[3].0..f[3].1], &mt).unwrap_or(0) as u64); }
                if mode >= 3 { acc = acc.wrapping_add(unsafe { parse_ts_m16(&d[f[2].0..f[2].1], &mt) }.unwrap_or(0) as u64); }
                if mode >= 4 { acc = acc.wrapping_add(h64(&d[f[1].0..f[1].1])); }
                if mode >= 5 {
                    let ty = &d[f[4].0..f[4].1];
                    acc = acc.wrapping_add(match ty { b"view" | b"cart" => 1, b"purchase" => parse_price(&d[f[6].0..f[6].1]).unwrap_or(0) as u64, _ => 3 });
                }
            });
        }
        let el = t.elapsed().as_secs_f64() / reps as f64;
        eprintln!("mode {mode}: {:.1} ns/line [{acc}]", el * 1e9 / lines as f64);
    }
}

fn main() {
    if let Ok(p) = std::env::var("MICROBENCH") {
        microbench(&p);
        return;
    }
    let t0 = std::time::Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let outdir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .max(1);
    let verbose = std::env::var("VERBOSE").is_ok();
    let stop_at: usize = std::env::var("STOP").ok().and_then(|s| s.parse().ok()).unwrap_or(99);
    let nlap = std::cell::Cell::new(0usize);
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:>8.3}s {}", t0.elapsed().as_secs_f64(), what);
        }
        nlap.set(nlap.get() + 1);
        if nlap.get() >= stop_at {
            std::process::exit(0);
        }
    };

    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = parse_manifest_ts(&man, "\"window_start\"");
    let we = parse_manifest_ts(&man, "\"window_end\"");

    let prods = read_small(&data.join("products.csv"));
    let mut catnames: Vec<Vec<u8>> = prods.iter().map(|p| p.1.clone()).collect();
    catnames.push(b"unknown".to_vec());
    catnames.sort();
    catnames.dedup();
    assert!(catnames.len() <= 255, "too many categories");
    let catidx = |c: &[u8]| catnames.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u8;
    let mut prod = U64Map::<u8>::with_capacity(prods.len());
    for (p, c) in &prods {
        let (i, _) = prod.entry(h64(p));
        *prod.val_mut(i) = catidx(c) + 1;
    }
    let unknown_cat = catidx(b"unknown");

    let camps_v = read_small(&data.join("campaigns.csv"));
    let mut camps = U64Map::<u16>::with_capacity(camps_v.len());
    for (k, (c, _)) in camps_v.iter().enumerate() {
        let (i, _) = camps.entry(h64(c));
        *camps.val_mut(i) = k as u16 + 1;
    }
    let ncamp = camps_v.len();
    assert!(ncamp < 65535);
    let ctx = Ctx { ws, we, prod, unknown_cat, camps, months: Months::new(ws), nopush: std::env::var_os("NOPUSH").is_some() };

    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let lk_paths = list_csv(&data.join("links"));
    let mut all: Vec<PathBuf> = Vec::new();
    all.extend(ev_paths.iter().cloned());
    all.extend(ck_paths.iter().cloned());
    all.extend(lk_paths.iter().cloned());
    assert!(all.len() <= 512, "too many input files");
    let maps: Vec<Option<Mmap>> = all.iter().map(|p| map_file(p)).collect();
    let handles: Vec<Option<File>> = all.iter().map(|p| File::open(p).ok()).collect();
    for m in maps.iter().flatten() {
        assert!(m.len() < (1usize << 31), "input file over 2 GB");
    }
    let files = Files { maps, handles };
    let ne = ev_paths.len();
    let nc = ck_paths.len();
    let ev_ids: Vec<usize> = (0..ne).collect();
    let ck_ids: Vec<usize> = (ne..ne + nc).collect();
    let lk_ids: Vec<usize> = (ne + nc..all.len()).collect();
    let fsize = |ids: &[usize]| -> usize { ids.iter().map(|&i| files.maps[i].as_ref().map(|m| m.len()).unwrap_or(0)).sum() };
    let ev_bytes = fsize(&ev_ids);
    let ck_bytes = fsize(&ck_ids);

    let mut lwork = Vec::new();
    chunks(&files, &lk_ids, Kind::Link, &mut lwork);
    let mut work = Vec::new();
    chunks(&files, &ev_ids, Kind::Event, &mut work);
    chunks(&files, &ck_ids, Kind::Click, &mut work);
    work.sort_by_key(|w| std::cmp::Reverse(w.3 - w.2));
    lap("setup");

    // ---- P1: all linked-ID work runs on one background thread (see build_links)


    // ---- P2: union-find thread || events + clicks parse
    // per-thread arenas: A = event copies (later person records), B = survivors, C = clicks
    let est_ev = ev_bytes / 60 + 1024;
    let est_ck = ck_bytes / 40 + 1024;
    let cap_a = est_ev * 32 + est_ck * 24 + est_ev * 24 + NEB * 2048 * 32 + 2 * NPB * 2048 * 24 + (64 << 20);
    let cap_b = est_ev * 24 + NUB * 2048 * 24 + (64 << 20);
    let cap_c = est_ck * 24 + NUB * 1024 * 24 + (64 << 20);
    let arenas: Vec<Mutex<[Arena; 3]>> = (0..nthreads).map(|_| Mutex::new([Arena::new(cap_a), Arena::new(cap_b), Arena::new(cap_c)])).collect();
    let (lbs, ncomp, wparts, ck_parts) = std::thread::scope(|outer| {
    let files_r = &files;
    let lwork_r = &lwork;
    let linker = outer.spawn(move || {
        let t = std::time::Instant::now();
        let r = build_links(files_r, lwork_r);
        if verbose {
            eprintln!("   links thread took {:.3}s", t.elapsed().as_secs_f64());
        }
        r
    });
    let next = AtomicUsize::new(0);
    let parsed = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let ctx = &ctx;
                let next = &next;
                let work = &work;
                let files = &files;
                let arenas = &arenas;
                s.spawn(move || {
                    let mut ar = arenas[t].lock().unwrap();
                    let [aa, _, ac] = &mut *ar;
                    let mut ebk: Scat<ERec, 8> = Scat::new(NEB, 2048);
                    let mut cbk: Scat<CRec, 8> = Scat::new(NUB, 1024);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= work.len() {
                            break;
                        }
                        let (kind, fi, st, en) = work[i];
                        match kind {
                            Kind::Event => parse_events(files, fi, st, en, ctx, &mut ebk, aa),
                            Kind::Click => parse_clicks(files, fi, st, en, ctx, &mut cbk, ac),
                            Kind::Link => unreachable!(),
                        }
                    }
                    (ebk.finish(aa), cbk.finish(ac))
                })
            })
            .collect();
        let parsed: Vec<_> = hs.into_iter().map(|h| h.join().unwrap()).collect();
        lap("events/clicks parsed");
        parsed
    });
    let (ev_parts, ck_parts): (Vec<Store<ERec>>, Vec<Store<CRec>>) = parsed.into_iter().unzip();

    // ---- P3: dedup per event bucket -> survivors by user bucket
    let next = AtomicUsize::new(0);
    let wparts: Vec<Store<WRec>> = par(nthreads, |t| {
        let mut ar = arenas[t].lock().unwrap();
        let [_, abw, _] = &mut *ar;
        let mut wb: Scat<WRec, 8> = Scat::new(NUB, 2048);
        let mut tab: Vec<DEnt> = Vec::new();
        let mut ins: Vec<u32> = Vec::new();
        loop {
            let b = next.fetch_add(1, Ordering::Relaxed);
            if b >= NEB {
                break;
            }
            dedup_bucket(&ev_parts, b, &mut tab, &mut ins, &mut wb, abw);
        }
        wb.finish(abw)
    });
    drop(ev_parts);
    lap("dedup");
    let (lbs, ncomp) = linker.join().unwrap();
    lap("links joined");
    (lbs, ncomp, wparts, ck_parts)
    });

    // ---- P4: per user bucket. Linked IDs -> component id, scattered by component range;
    // persons with a single unlinked ID are complete here and sessionized at once.
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws.div_euclid(DAY_MS) + 1).max(1) as usize;
    let day0 = ws.div_euclid(DAY_MS);
    let ncats = catnames.len();
    let mut rb = 0u32;
    while (ncomp >> rb) >= NPB {
        rb += 1;
    }
    let npb_used = (ncomp >> rb) + 1;
    let sess: Vec<Mutex<Sess>> = (0..nthreads).map(|_| Mutex::new(Sess::new(ncamp, ncats, ndays))).collect();
    let next = AtomicUsize::new(0);
    let pparts: Vec<(Store<PRec>, Store<CRec>)> = par(nthreads, |t| {
        let mut ar = arenas[t].lock().unwrap();
        let [aa, _, _] = &mut *ar;
        aa.reset();
        let mut ss = sess[t].lock().unwrap();
        let ss = &mut *ss;
        let mut pe: Scat<PRec, 8> = Scat::new(npb_used, 2048);
        let mut pc: Scat<CRec, 8> = Scat::new(npb_used, 1024);
        let mut ue: Vec<PRec> = Vec::new();
        let mut uc: Vec<CRec> = Vec::new();
        loop {
            let b = next.fetch_add(1, Ordering::Relaxed);
            if b >= NUB {
                break;
            }
            let lb = &lbs[b];
            ue.clear();
            uc.clear();
            for t in &wparts {
                t.for_each_slice(b, |sl| {
                    for r in sl {
                        match lb.map.get(r.user) {
                            Some(&i) => {
                                let (pid, nloc) = lb.person[i as usize];
                                pe.push(aa, (pid >> rb) as usize, PRec { pkey: pid, b: (r.b & !LOC_MASK) | nloc, c: r.c });
                            }
                            None => ue.push(PRec { pkey: r.user, b: r.b, c: r.c }),
                        }
                    }
                });
            }
            for t in &ck_parts {
                t.for_each_slice(b, |sl| {
                    for c in sl {
                        match lb.map.get(c.key) {
                            Some(&i) => {
                                let pid = lb.person[i as usize].0;
                                pc.push(aa, (pid >> rb) as usize, CRec { key: pid, ..*c });
                            }
                            None => uc.push(*c),
                        }
                    }
                });
            }
            ss.g.group_hashed(&ue, &uc);
            ss.run(&files, ws, day0, ndays, ncamp);
        }
        (pe.finish(aa), pc.finish(aa))
    });
    drop(wparts);
    drop(ck_parts);
    drop(lbs);
    let (pev, pck): (Vec<Store<PRec>>, Vec<Store<CRec>>) = pparts.into_iter().unzip();
    lap("person mapping + single-ID persons");

    // ---- P5: linked persons per component range
    let mut order: Vec<usize> = (0..npb_used).collect();
    let sizes: Vec<usize> = (0..npb_used).map(|b| pev.iter().map(|t| t.len(b)).sum()).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    let next = AtomicUsize::new(0);
    par(nthreads, |t| {
        let mut ss = sess[t].lock().unwrap();
        let ss = &mut *ss;
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= npb_used {
                break;
            }
            let b = order[i];
            ss.g.group_dense(&pev, &pck, b, (b as u64) << rb, 1usize << rb);
            ss.run(&files, ws, day0, ndays, ncamp);
        }
    });
    drop(pev);
    drop(pck);
    let results: Vec<(Vec<UserRow>, Vec<CampAgg>)> = sess.into_iter().map(|m| { let s = m.into_inner().unwrap(); (s.rows, s.camp) }).collect();
    lap("sessionized");

    // ---- P6: output
    let mut camp_tot = vec![CampAgg::default(); (ncamp + 1) * ndays];
    for (_, c) in &results {
        for (t, x) in camp_tot.iter_mut().zip(c) {
            t.sessions += x.sessions;
            t.purch += x.purch;
            t.revenue += x.revenue;
            t.assisted += x.assisted;
        }
    }
    let camp_names: Vec<(&[u8], &[u8])> = camps_v
        .iter()
        .map(|(a, b)| (a.as_slice(), b.as_slice()))
        .chain(std::iter::once((&b"organic"[..], &b"organic"[..])))
        .collect();
    let udir = outdir.join("user_daily");
    let cdir = outdir.join("campaign_daily");
    std::fs::create_dir_all(&udir).unwrap();
    std::fs::create_dir_all(&cdir).unwrap();
    let err = Mutex::new(None::<String>);
    std::thread::scope(|s| {
        for (k, (rows, _)) in results.iter().enumerate() {
            let files = &files;
            let catnames = &catnames;
            let udir = &udir;
            let err = &err;
            s.spawn(move || {
                if let Err(e) = out::write_user(&udir.join(format!("part-{k:03}.parquet")), rows, files, catnames) {
                    *err.lock().unwrap() = Some(e.to_string());
                }
            });
        }
        if let Err(e) = out::write_camp(&cdir.join("part-000.parquet"), &camp_tot, &camp_names, ndays, day0) {
            *err.lock().unwrap() = Some(e.to_string());
        }
    });
    if let Some(e) = err.into_inner().unwrap() {
        eprintln!("write error: {e}");
        std::process::exit(1);
    }
    lap("written");
    std::process::exit(0);
}

// ---------------------------------------------------------------- linked IDs

/// One side of a link line: ID hash, and loc (40 bits) | edge endpoint index << 40.
#[derive(Clone, Copy)]
#[repr(C)]
struct Side {
    h: u64,
    x: u64,
    pre: u64, // first 8 bytes, big-endian, zero padded
}

#[inline(always)]
fn prefix8(s: &[u8]) -> u64 {
    if s.len() >= 8 {
        u64::from_be_bytes(s[..8].try_into().unwrap())
    } else {
        let mut buf = [0u8; 8];
        buf[..s.len()].copy_from_slice(s);
        u64::from_be_bytes(buf)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct SlotGid {
    slot: u32,
    gid: u32,
}

/// Parse links, assign dense IDs per user bucket, union-find, name each person by
/// its byte-wise smallest ID. Single-threaded; runs concurrently with event work.
fn build_links(files: &Files, lwork: &[(Kind, usize, usize, usize)]) -> (Vec<LinkBucket>, usize) {
    let t0 = std::time::Instant::now();
    let vb = std::env::var_os("VERBOSE").is_some();
    let lap = |w: &str| if vb { eprintln!("     links {w}: {:.3}s", t0.elapsed().as_secs_f64()) };
    let mut ar = Arena::new(0);
    let mut sc: Scat<Side, 8> = Scat::new(NUB, 512);
    let mut nend = 0u64; // edge endpoints
    for &(_, fi, st, en) in lwork {
        files.with_chunk(fi, st, en, false, |d, base| {
            scan::<2>(d, 0, d.len(), |f| {
                let a = &d[f[0].0..f[0].1];
                let b = &d[f[1].0..f[1].1];
                if a.is_empty() || b.is_empty() {
                    return;
                }
                let ha = h64(a);
                let hb = h64(b);
                sc.push(&mut ar, ub(ha), Side { h: ha, x: mkloc(fi, base + f[0].0) | (nend << 40), pre: prefix8(a) });
                sc.push(&mut ar, ub(hb), Side { h: hb, x: mkloc(fi, base + f[1].0) | ((nend + 1) << 40), pre: prefix8(b) });
                nend += 2;
            });
        });
    }
    assert!(nend < (1u64 << 24), "too many links");
    let store = sc.finish(&mut ar);
    lap("parsed");
    // (slot, gid) pairs scattered by slot range so that filling `edges` stays cache-local
    let mut sh = 14u32;
    while (nend >> sh) >= 1024 {
        sh += 1;
    }
    let nparts = ((nend >> sh) + 1) as usize;
    let mut sg: Scat<SlotGid, 8> = Scat::new(nparts, 1024);
    let mut gpre: Vec<u64> = Vec::new();
    let mut lbs: Vec<LinkBucket> = Vec::with_capacity(NUB);
    let mut gid = 0u32;
    for b in 0..NUB {
        let n = store.len(b);
        let mut map = U64Map::<u32>::with_capacity(n / 2 + 8);
        let mut hashes = Vec::with_capacity(n / 2 + 8);
        let mut locs = Vec::with_capacity(n / 2 + 8);
        let base = gid;
        store.for_each_slice(b, |sl| {
            for s in sl {
                let (i, ex) = map.entry(s.h);
                if !ex {
                    *map.val_mut(i) = hashes.len() as u32;
                    hashes.push(s.h);
                    locs.push(s.x & LOC_MASK);
                    gpre.push(s.pre);
                }
                let slot = (s.x >> 40) as u32;
                sg.push(&mut ar, (slot >> sh) as usize, SlotGid { slot, gid: base + map.vals[i] });
            }
        });
        gid += hashes.len() as u32;
        lbs.push(LinkBucket { map, hashes, locs, person: Vec::new() });
    }
    let nids = gid as usize;
    lap("ids-maps");
    let sgs = sg.finish(&mut ar);
    let mut edges = vec![0u32; nend as usize];
    for p in 0..nparts {
        sgs.for_each_slice(p, |sl| {
            for x in sl {
                edges[x.slot as usize] = x.gid;
            }
        });
    }
    lap("ids");
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    #[inline(always)]
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for e in edges.chunks_exact(2) {
        let (a, b) = (e[0], e[1]);
        if a == b {
            continue;
        }
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(edges);
    lap("uf");
    // name: byte-wise smallest ID per root, compared by an 8-byte big-endian prefix first
    let mut gloc: Vec<u64> = Vec::with_capacity(nids);
    let mut ghash: Vec<u64> = Vec::with_capacity(nids);
    for lb in &lbs {
        gloc.extend_from_slice(&lb.locs);
        ghash.extend_from_slice(&lb.hashes);
    }
    let mut best: Vec<u32> = (0..nids as u32).collect();
    for x in 0..nids as u32 {
        let r = find(&mut parent, x);
        if r != x {
            let cur = best[r as usize] as usize;
            let xi = x as usize;
            let less = match gpre[xi].cmp(&gpre[cur]) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => files.field(gloc[xi]) < files.field(gloc[cur]),
            };
            if less {
                best[r as usize] = x;
            }
        }
    }
    lap("names");
    // dense component ids
    let mut comp: Vec<u32> = vec![u32::MAX; nids];
    let mut ncomp = 0u32;
    let mut x = 0u32;
    for lb in lbs.iter_mut() {
        let n = lb.hashes.len();
        lb.person = Vec::with_capacity(n);
        for _ in 0..n {
            let r = find(&mut parent, x) as usize;
            if comp[r] == u32::MAX {
                comp[r] = ncomp;
                ncomp += 1;
            }
            let b = best[r] as usize;
            lb.person.push((comp[r] as u64, gloc[b]));
            x += 1;
        }
    }
    let _ = &ghash;
    lap("persons");
    (lbs, ncomp as usize)
}

// ---------------------------------------------------------------- events

#[inline(always)]
fn parse_events(files: &Files, fi: usize, st: usize, en: usize, ctx: &Ctx, ebk: &mut Scat<ERec, 8>, ar: &mut Arena) {
    files.with_chunk(fi, st, en, false, |d, base| parse_events_in(d, base, fi, 0, d.len(), ctx, ebk, ar))
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn parse_events_in(d: &[u8], base: usize, fi: usize, st: usize, en: usize, ctx: &Ctx, ebk: &mut Scat<ERec, 8>, ar: &mut Arena) {
    let ws = ctx.ws;
    let we = ctx.we;
    let ws_s = ws.div_euclid(1000);
    let nopush = ctx.nopush;
    let mut sink = 0u64;
    scan::<7>(d, st, en, |f| {
        let eid = &d[f[0].0..f[0].1];
        if eid.is_empty() {
            return;
        }
        let ingest_ms = parse_ts_f1(&d[f[3].0..f[3].1], &ctx.months).unwrap_or(i64::MAX / 4);
        let ingest = (ingest_ms.div_euclid(1000) - ws_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32;
        let h = h64(eid);
        let mut user_h = 0u64;
        let mut b = 0u64;
        let mut ts32 = TS_INVALID;
        let mut cat = 0u64;
        'v: {
            let user = &d[f[1].0..f[1].1];
            if user.is_empty() {
                break 'v;
            }
            let Some(ts) = (unsafe { parse_ts_m16(&d[f[2].0..f[2].1], &ctx.months) }) else { break 'v };
            if ts < ws || ts >= we || ingest_ms - ts > DAY_MS {
                break 'v;
            }
            let ty = &d[f[4].0..f[4].1];
            if f[5].0 == f[5].1 {
                break 'v;
            }
            match ty {
                b"view" | b"cart" => {}
                b"purchase" => {
                    let Some(p) = parse_price(&d[f[6].0..f[6].1]) else { break 'v };
                    let product = &d[f[5].0..f[5].1];
                    cat = match ctx.prod.get(h64(product)) {
                        Some(&c) => (c - 1) as u64,
                        None => ctx.unknown_cat as u64,
                    };
                    b = (p as u64) << 40;
                }
                _ => break 'v,
            }
            user_h = h64(user);
            b |= mkloc(fi, base + f[1].0);
            ts32 = (ts - ws) as u32;
        }
        let r = ERec { a: (h & EID_MASK) | (cat << 56), user: user_h, b, c: (ts32 as u64) | ((ingest as u32 as u64) << 32) };
        if nopush { sink ^= r.a ^ r.user ^ r.b ^ r.c; } else { ebk.push(ar, eb(h), r); }
    });
    if sink == 42 { eprintln!("."); }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct DEnt {
    key: u64,
    rec: *const ERec,
}
impl Default for DEnt {
    fn default() -> Self {
        DEnt { key: 0, rec: std::ptr::null() }
    }
}

fn dedup_bucket(parts: &[Store<ERec>], b: usize, tab: &mut Vec<DEnt>, ins: &mut Vec<u32>, wb: &mut Scat<WRec, 8>, ar: &mut Arena) {
    let n: usize = parts.iter().map(|p| p.len(b)).sum();
    let cap = (n * 3 / 2 + 16).next_power_of_two();
    // the table is all-empty between calls (used slots are cleared at the end)
    if tab.len() < cap {
        tab.clear();
        tab.resize(cap, DEnt::default());
    }
    ins.clear();
    let mask = cap - 1;
    // key = (eid low bits) + 1, never 0 (cat sits in bits 56.. of `a`, above EID_MASK)
    for p in parts {
        p.for_each_slice(b, |sl| {
            for r in sl {
                let key = (r.a & EID_MASK) + 1;
                let mut i = (key as usize) & mask;
                loop {
                    let e = unsafe { tab.get_unchecked_mut(i) };
                    if e.key == key {
                        // keep the copy with the smaller ingest_ts
                        let cur = unsafe { &*e.rec };
                        if ((r.c >> 32) as i32) < ((cur.c >> 32) as i32) {
                            e.rec = r;
                        }
                        break;
                    }
                    if e.key == 0 {
                        *e = DEnt { key, rec: r };
                        ins.push(i as u32);
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
        });
    }
    for &i in ins.iter() {
        let e = unsafe { tab.get_unchecked_mut(i as usize) };
        e.key = 0;
        let r = unsafe { &*e.rec };
        let ts = r.c as u32;
        if ts != TS_INVALID {
            wb.push(ar, ub(r.user), WRec { user: r.user, b: r.b, c: ts as u64 | ((r.a >> 56) << 32) });
        }
    }
}

// ---------------------------------------------------------------- clicks

#[inline(always)]
fn parse_clicks(files: &Files, fi: usize, st: usize, en: usize, ctx: &Ctx, cbk: &mut Scat<CRec, 8>, ar: &mut Arena) {
    files.with_chunk(fi, st, en, false, |d, base| parse_clicks_in(d, base, fi, 0, d.len(), ctx, cbk, ar))
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn parse_clicks_in(d: &[u8], base: usize, fi: usize, st: usize, en: usize, ctx: &Ctx, cbk: &mut Scat<CRec, 8>, ar: &mut Arena) {
    let lo = ctx.ws - WEEK_MS;
    scan::<4>(d, st, en, |f| {
        if f[0].0 == f[0].1 || f[1].0 == f[1].1 {
            return;
        }
        let Some(ts) = (unsafe { parse_ts_m16(&d[f[3].0..f[3].1], &ctx.months) }) else { return };
        if ts < lo || ts >= ctx.we {
            return;
        }
        let Some(&c) = ctx.camps.get(h64(&d[f[2].0..f[2].1])) else { return };
        let h = h64(&d[f[1].0..f[1].1]);
        cbk.push(ar, ub(h), CRec { key: h, b: mkloc(fi, base + f[0].0) | (((c - 1) as u64) << 40), ts: ts - ctx.ws });
    });
}

// ---------------------------------------------------------------- grouping

/// Per-thread sessionization state and outputs.
struct Sess {
    g: Grouper,
    st: SessState,
    rows: Vec<UserRow>,
    camp: Vec<CampAgg>,
}

impl Sess {
    fn new(ncamp: usize, ncats: usize, ndays: usize) -> Self {
        Sess { g: Grouper::default(), st: SessState::new(ncamp, ncats), rows: Vec::new(), camp: vec![CampAgg::default(); (ncamp + 1) * ndays] }
    }
    fn run(&mut self, files: &Files, ws: i64, day0: i64, ndays: usize, ncamp: usize) {
        for l in 0..self.g.npersons() {
            let (e, c) = self.g.person(l);
            if e.is_empty() {
                continue;
            }
            sessionize(files, e, c, ws, day0, ndays, ncamp, &mut self.st, &mut self.rows, &mut self.camp);
        }
    }
}

/// Groups events and clicks by person (counting sort by a dense local id), each
/// person's run sorted by ts.
#[derive(Default)]
struct Grouper {
    map: Option<U64Map<u32>>,
    np: usize,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    elid: Vec<u32>,
    clid: Vec<u32>,
    ev: Vec<PRec>,
    ck: Vec<CRec>,
}

impl Grouper {
    /// Records keyed by an arbitrary 64-bit person key (hash map assigns local ids).
    fn group_hashed(&mut self, ev: &[PRec], ck: &[CRec]) {
        let map = self.map.get_or_insert_with(|| U64Map::with_capacity(1 << 12));
        map.keys.iter_mut().for_each(|k| *k = 0);
        map.len = 0;
        self.elid.clear();
        self.clid.clear();
        let mut np = 0u32;
        for r in ev {
            let (i, ex) = map.entry(r.pkey);
            if !ex {
                *map.val_mut(i) = np;
                np += 1;
            }
            self.elid.push(map.vals[i]);
        }
        for c in ck {
            self.clid.push(match map.get(c.key) {
                Some(&l) => l,
                None => u32::MAX,
            });
        }
        self.np = np as usize;
        let elid = std::mem::take(&mut self.elid);
        let clid = std::mem::take(&mut self.clid);
        self.place(ev.len(), |f| ev.iter().zip(&elid).for_each(|(r, &l)| f(l, r)), |f| ck.iter().zip(&clid).for_each(|(c, &l)| f(l, c)));
        self.elid = elid;
        self.clid = clid;
    }

    /// Records keyed by a dense id in [base, base + range).
    fn group_dense(&mut self, pev: &[Store<PRec>], pck: &[Store<CRec>], b: usize, base: u64, range: usize) {
        self.np = range;
        let n: usize = pev.iter().map(|t| t.len(b)).sum();
        self.place(
            n,
            |f| {
                for t in pev {
                    t.for_each_slice(b, |sl| sl.iter().for_each(|r| f((r.pkey - base) as u32, r)));
                }
            },
            |f| {
                for t in pck {
                    t.for_each_slice(b, |sl| sl.iter().for_each(|c| f((c.key - base) as u32, c)));
                }
            },
        );
    }

    /// Counting sort into self.ev / self.ck by local id, then sort each run by ts.
    fn place(&mut self, n: usize, evs: impl Fn(&mut dyn FnMut(u32, &PRec)), cks: impl Fn(&mut dyn FnMut(u32, &CRec))) {
        let np = self.np;
        self.eoff.clear();
        self.eoff.resize(np + 1, 0);
        self.coff.clear();
        self.coff.resize(np + 1, 0);
        {
            let eoff = &mut self.eoff;
            evs(&mut |l, _| eoff[l as usize + 1] += 1);
            let coff = &mut self.coff;
            cks(&mut |l, _| {
                if l != u32::MAX {
                    coff[l as usize + 1] += 1
                }
            });
        }
        for i in 0..np {
            self.eoff[i + 1] += self.eoff[i];
            self.coff[i + 1] += self.coff[i];
        }
        debug_assert_eq!(self.eoff[np] as usize, n);
        self.ev.clear();
        self.ev.reserve(n);
        unsafe { self.ev.set_len(self.eoff[np] as usize) };
        let ncl = self.coff[np] as usize;
        self.ck.clear();
        self.ck.reserve(ncl);
        unsafe { self.ck.set_len(ncl) };
        {
            let mut pos: Vec<u32> = self.eoff[..np].to_vec();
            let evd = &mut self.ev;
            evs(&mut |l, r| {
                unsafe { *evd.get_unchecked_mut(*pos.get_unchecked(l as usize) as usize) = *r };
                pos[l as usize] += 1;
            });
            let mut pos: Vec<u32> = self.coff[..np].to_vec();
            let ckd = &mut self.ck;
            cks(&mut |l, c| {
                if l != u32::MAX {
                    unsafe { *ckd.get_unchecked_mut(*pos.get_unchecked(l as usize) as usize) = *c };
                    pos[l as usize] += 1;
                }
            });
        }
        for l in 0..np {
            let (a, z) = (self.eoff[l] as usize, self.eoff[l + 1] as usize);
            sort_small(&mut self.ev[a..z], |r| r.c as u32 as i64);
            let (a, z) = (self.coff[l] as usize, self.coff[l + 1] as usize);
            sort_small(&mut self.ck[a..z], |r| r.ts);
        }
    }
    fn npersons(&self) -> usize {
        self.np
    }
    fn person(&self, l: usize) -> (&[PRec], &[CRec]) {
        (
            &self.ev[self.eoff[l] as usize..self.eoff[l + 1] as usize],
            &self.ck[self.coff[l] as usize..self.coff[l + 1] as usize],
        )
    }
}

#[inline]
fn sort_small<T: Copy>(v: &mut [T], key: impl Fn(&T) -> i64) {
    if v.len() <= 24 {
        for i in 1..v.len() {
            let x = v[i];
            let kx = key(&x);
            let mut j = i;
            while j > 0 && key(&v[j - 1]) > kx {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    } else {
        v.sort_unstable_by_key(|r| key(r));
    }
}

// ---------------------------------------------------------------- sessionize

struct SessState {
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catrev: Vec<u64>,
    touched: Vec<u8>,
}

impl SessState {
    fn new(ncamp: usize, ncats: usize) -> Self {
        SessState { cnt: vec![0; ncamp + 1], pos: vec![0; ncamp + 1], active: Vec::new(), catrev: vec![0; ncats], touched: Vec::new() }
    }
    #[inline(always)]
    fn add(&mut self, c: u32) {
        let ci = c as usize;
        if self.cnt[ci] == 0 {
            self.pos[ci] = self.active.len() as u32;
            self.active.push(c);
        }
        self.cnt[ci] += 1;
    }
    #[inline(always)]
    fn remove(&mut self, c: u32) {
        let ci = c as usize;
        self.cnt[ci] -= 1;
        if self.cnt[ci] == 0 {
            let p = self.pos[ci] as usize;
            let last = *self.active.last().unwrap();
            self.active[p] = last;
            self.pos[last as usize] = p as u32;
            self.active.pop();
        }
    }
    fn clear(&mut self) {
        for &c in &self.active {
            self.cnt[c as usize] = 0;
        }
        self.active.clear();
    }
}

#[inline(always)]
fn camp_of(c: &CRec) -> u32 {
    (c.b >> 40) as u32
}

#[allow(clippy::too_many_arguments)]
fn sessionize(
    files: &Files,
    ev: &[PRec],
    ck: &[CRec],
    ws: i64,
    day0: i64,
    ndays: usize,
    ncamp: usize,
    st: &mut SessState,
    rows: &mut Vec<UserRow>,
    camp: &mut [CampAgg],
) {
    let organic = ncamp as u32;
    let mut ci = 0usize;
    let mut i = 0usize;
    let tsof = |r: &PRec| (r.c as u32) as i64;
    while i < ev.len() {
        let pkey = ev[i].pkey;
        let mut j = i + 1;
        while j < ev.len() && ev[j].pkey == pkey {
            j += 1;
        }
        while ci < ck.len() && ck[ci].key < pkey {
            ci += 1;
        }
        let cs = ci;
        while ci < ck.len() && ck[ci].key == pkey {
            ci += 1;
        }
        let cl = &ck[cs..ci];
        let nloc = ev[i].b & LOC_MASK;
        let (mut lo, mut hi) = (0usize, 0usize);

        let mut cur_day: i64 = -1;
        let mut r_sess = 0u32;
        let mut r_ev = 0u32;
        let mut r_rev = 0u64;

        let mut k = i;
        while k < j {
            let s = tsof(&ev[k]);
            let mut m = k + 1;
            let mut last = s;
            while m < j && tsof(&ev[m]) - last <= GAP_MS {
                last = tsof(&ev[m]);
                m += 1;
            }
            let abs_day = (ws + s).div_euclid(DAY_MS);
            let di = (abs_day - day0) as usize;
            let credit = if cl.is_empty() {
                organic
            } else {
                while hi < cl.len() && cl[hi].ts <= s {
                    st.add(camp_of(&cl[hi]));
                    hi += 1;
                }
                while lo < hi && cl[lo].ts < s - WEEK_MS {
                    st.remove(camp_of(&cl[lo]));
                    lo += 1;
                }
                for &a in &st.active {
                    camp[a as usize * ndays + di].assisted += 1;
                }
                if lo < hi {
                    let t = cl[hi - 1].ts;
                    let mut best = hi - 1;
                    let mut q = hi - 1;
                    while q > lo && cl[q - 1].ts == t {
                        q -= 1;
                        if files.field(cl[q].b & LOC_MASK) < files.field(cl[best].b & LOC_MASK) {
                            best = q;
                        }
                    }
                    camp_of(&cl[best])
                } else {
                    organic
                }
            };
            if abs_day != cur_day {
                if cur_day >= 0 {
                    flush_row(st, rows, nloc, cur_day, r_sess, r_ev, r_rev);
                }
                cur_day = abs_day;
                r_sess = 0;
                r_ev = 0;
                r_rev = 0;
            }
            let mut srev = 0u64;
            let mut purch = false;
            for e in &ev[k..m] {
                let price = e.b >> 40;
                if price != 0 {
                    purch = true;
                    srev += price;
                    let c = ((e.c >> 32) & 0xff) as usize;
                    if st.catrev[c] == 0 {
                        st.touched.push(c as u8);
                    }
                    st.catrev[c] += price;
                }
            }
            let ca = &mut camp[credit as usize * ndays + di];
            ca.sessions += 1;
            ca.revenue += srev;
            ca.purch += purch as u64;
            r_sess += 1;
            r_ev += (m - k) as u32;
            r_rev += srev;
            k = m;
        }
        if cur_day >= 0 {
            flush_row(st, rows, nloc, cur_day, r_sess, r_ev, r_rev);
        }
        st.clear();
        i = j;
    }
}

#[inline]
fn flush_row(st: &mut SessState, rows: &mut Vec<UserRow>, nloc: u64, day: i64, s: u32, e: u32, rev: u64) {
    let mut top = 255u8;
    let mut best = 0u64;
    for &c in &st.touched {
        let v = st.catrev[c as usize];
        if v > best || (v == best && c < top) {
            best = v;
            top = c;
        }
        st.catrev[c as usize] = 0;
    }
    st.touched.clear();
    rows.push(UserRow { nloc, day: day as i32, sessions: s, events: e, revenue: rev, top });
}

#[cfg(test)]
mod tests;
