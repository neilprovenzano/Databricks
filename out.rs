// Minimal Parquet writer: one row group, one uncompressed PLAIN data page (v1) per
// column, Thrift compact-protocol metadata.

use crate::{CampAgg, Files, UserRow};
use std::io::Write;
use std::path::Path;

// ---------------------------------------------------------------- thrift compact

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BIN: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl T {
    fn new() -> T {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }
    fn fh(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            let z = T::zz(id as i64);
            self.varint(z);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.fh(id, T_I32);
        self.varint(T::zz(v as i64));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.fh(id, T_I64);
        self.varint(T::zz(v));
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.fh(id, T_BIN);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.fh(id, T_STRUCT);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, n: usize, ety: u8) {
        self.fh(id, T_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
    /// begin a struct that is a list element (closed with end())
    fn elem(&mut self) {
        self.last.push(0);
    }
}

// ---------------------------------------------------------------- columns

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    OptStr,
    I64,
    Date,
}

struct Col {
    name: &'static str,
    kind: Kind,
    data: Vec<u8>,
    defs: Vec<bool>,
}

impl Col {
    fn new(name: &'static str, kind: Kind, cap: usize) -> Col {
        Col { name, kind, data: Vec::with_capacity(cap), defs: Vec::new() }
    }
    #[inline]
    fn str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
    }
    #[inline]
    fn opt_str(&mut self, s: Option<&[u8]>) {
        match s {
            Some(v) => {
                self.defs.push(true);
                self.str(v);
            }
            None => self.defs.push(false),
        }
    }
    #[inline]
    fn i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
}

fn def_levels(defs: &[bool]) -> Vec<u8> {
    // RLE/bit-packed hybrid, bit width 1, a single bit-packed run
    let n = defs.len();
    let groups = n.div_ceil(8);
    let mut rle = Vec::with_capacity(groups + 8);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        rle.push((h as u8) | 0x80);
        h >>= 7;
    }
    rle.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for i in 0..8 {
            let k = g * 8 + i;
            if k < n && defs[k] {
                byte |= 1 << i;
            }
        }
        rle.push(byte);
    }
    let mut out = Vec::with_capacity(rle.len() + 4);
    out.extend_from_slice(&(rle.len() as u32).to_le_bytes());
    out.extend_from_slice(&rle);
    out
}

fn ptype(k: Kind) -> i32 {
    match k {
        Kind::Str | Kind::OptStr => 6, // BYTE_ARRAY
        Kind::I64 => 2,                // INT64
        Kind::Date => 1,               // INT32
    }
}

fn write_file(path: &Path, cols: &[Col], nrows: usize) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::with_capacity(4 << 20, std::fs::File::create(path)?);
    f.write_all(b"PAR1")?;
    let mut off: u64 = 4;
    let mut meta: Vec<(u64, u64)> = Vec::new(); // (data page offset, chunk size)
    if nrows > 0 {
        for c in cols {
            let defs = if c.kind == Kind::OptStr { def_levels(&c.defs) } else { Vec::new() };
            let page_len = defs.len() + c.data.len();
            let mut h = T::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, page_len as i32);
            h.i32(3, page_len as i32);
            h.begin(5);
            h.i32(1, nrows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            f.write_all(&h.b)?;
            f.write_all(&defs)?;
            f.write_all(&c.data)?;
            let total = (h.b.len() + page_len) as u64;
            meta.push((off, total));
            off += total;
        }
    }
    let mut m = T::new();
    m.i32(1, 1); // version
    m.list(2, cols.len() + 1, T_STRUCT);
    m.elem();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.elem();
        m.i32(1, ptype(c.kind));
        m.i32(3, if c.kind == Kind::OptStr { 1 } else { 0 });
        m.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Str | Kind::OptStr => {
                m.i32(6, 0); // converted type UTF8
                m.begin(10); // logical type
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Kind::Date => {
                m.i32(6, 6); // converted type DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
            Kind::I64 => {}
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    if nrows > 0 {
        m.list(4, 1, T_STRUCT);
        m.elem();
        m.list(1, cols.len(), T_STRUCT);
        let mut total = 0i64;
        for (c, &(po, sz)) in cols.iter().zip(&meta) {
            total += sz as i64;
            m.elem();
            m.i64(2, po as i64); // file_offset
            m.begin(3); // ColumnMetaData
            m.i32(1, ptype(c.kind));
            m.list(2, 2, T_I32);
            m.varint(T::zz(0)); // PLAIN
            m.varint(T::zz(3)); // RLE
            m.list(3, 1, T_BIN);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, nrows as i64);
            m.i64(6, sz as i64);
            m.i64(7, sz as i64);
            m.i64(9, po as i64);
            m.end();
            m.end();
        }
        m.i64(2, total);
        m.i64(3, nrows as i64);
        m.end();
    } else {
        m.list(4, 0, T_STRUCT);
    }
    m.bin(6, b"sessions");
    m.b.push(0);
    f.write_all(&m.b)?;
    f.write_all(&(m.b.len() as u32).to_le_bytes())?;
    f.write_all(b"PAR1")?;
    f.flush()?;
    Ok(())
}

pub fn write_user(path: &Path, rows: &[UserRow], files: &Files, cats: &[Vec<u8>]) -> std::io::Result<()> {
    let n = rows.len();
    let mut cols = vec![
        Col::new("user_id", Kind::Str, n * 14),
        Col::new("day", Kind::Date, n * 4),
        Col::new("sessions", Kind::I64, n * 8),
        Col::new("events", Kind::I64, n * 8),
        Col::new("revenue_cents", Kind::I64, n * 8),
        Col::new("top_category", Kind::OptStr, n * 10),
    ];
    for r in rows {
        cols[0].str(files.field(r.nloc));
        cols[1].i32(r.day);
        cols[2].i64(r.sessions as i64);
        cols[3].i64(r.events as i64);
        cols[4].i64(r.revenue as i64);
        cols[5].opt_str(if r.top == 255 { None } else { Some(&cats[r.top as usize]) });
    }
    write_file(path, &cols, n)
}

pub fn write_camp(path: &Path, agg: &[CampAgg], names: &[(&[u8], &[u8])], ndays: usize, day0: i64) -> std::io::Result<()> {
    let mut cols = vec![
        Col::new("campaign_id", Kind::Str, 0),
        Col::new("channel", Kind::Str, 0),
        Col::new("day", Kind::Date, 0),
        Col::new("sessions", Kind::I64, 0),
        Col::new("purchasing_sessions", Kind::I64, 0),
        Col::new("revenue_cents", Kind::I64, 0),
        Col::new("assisted_sessions", Kind::I64, 0),
    ];
    let mut n = 0;
    for (c, nm) in names.iter().enumerate() {
        for d in 0..ndays {
            let a = &agg[c * ndays + d];
            if a.sessions > 0 || a.assisted > 0 {
                cols[0].str(nm.0);
                cols[1].str(nm.1);
                cols[2].i32((day0 + d as i64) as i32);
                cols[3].i64(a.sessions as i64);
                cols[4].i64(a.purch as i64);
                cols[5].i64(a.revenue as i64);
                cols[6].i64(a.assisted as i64);
                n += 1;
            }
        }
    }
    write_file(path, &cols, n)
}
