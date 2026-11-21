
    use super::*;
    #[test]
    fn ts_fuzz() {
        let seeds: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2028-02-29T00:00:00Z", b"2027-02-29T00:00:00Z", b"2026-10-04T01:30:00+02:00", b"2026-10-04T01:30:00+23:59",
            b"2026-10-04T01:30:00-00:00", b"9999-12-31T23:59:59Z", b"0000-01-01T00:00:00Z", b"86400000",
            b"123456789012345678", b"2028-02-29T23:59:59+12:00", b"2028-03-01T00:00:00-10:00", b"2027-12-31T23:59:59Z", b"2025-02-28T12:00:00Z", b"1234567890123456789", b"12345678901234567890", b"000000000000000000001",
        ];
        let alphabet = b"0123456789-:TZtz+ .\"\r\xd9";
        let mut x: u64 = 0x1234567;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let mts: Vec<Months> = [b"2026-10-01T00:00:00Z" as &[u8], b"2028-02-21T00:00:00Z", b"2025-01-05T00:00:00Z", b"2027-12-30T00:00:00Z"]
            .iter().map(|w| Months::new(parse_ts_slow(w).unwrap())).collect();
        for s in &seeds {
            assert_eq!(parse_ts(s), parse_ts_slow(s), "{:?}", std::str::from_utf8(s));
        }
        for _ in 0..3_000_000 {
            let mut v = seeds[(rnd() % seeds.len() as u64) as usize].to_vec();
            let k = rnd() % 3 + 1;
            for _ in 0..k {
                let i = (rnd() % v.len() as u64) as usize;
                match rnd() % 4 {
                    0 => v[i] = alphabet[(rnd() % alphabet.len() as u64) as usize],
                    1 => v[i] = b'0' + (rnd() % 10) as u8,
                    2 => { v.remove(i); if v.is_empty() { v.push(b'1'); } }
                    _ => v.insert(i, b'0' + (rnd() % 10) as u8),
                }
            }
            assert_eq!(parse_ts(&v), parse_ts_slow(&v), "{:?}", String::from_utf8_lossy(&v));
            {
                let mut buf = v.clone();
                buf.extend_from_slice(&[b'7'; 16]);
                buf.extend_from_slice(&[b'7'; 32]);
                let got = unsafe { parse_ts_m16(&buf[..v.len()], &mts[0]) };
                assert_eq!(got, parse_ts_slow(&v), "m16 {:?}", String::from_utf8_lossy(&v));

            }
            for mt in &mts {
                assert_eq!(parse_ts_m(&v, mt), parse_ts_slow(&v), "{:?}", String::from_utf8_lossy(&v));
                assert_eq!(parse_ts_f1(&v, mt), parse_ts_slow(&v), "{:?}", String::from_utf8_lossy(&v));
            }
        }
    }
    #[test]
    fn scan_matches_simple() {
        // compare scan::<3> against a straightforward splitter on random lines
        let alphabet = b"ab,\"\n\r x";
        let mut x: u64 = 0x9876;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        for it in 0..20000 {
            let n = if it % 100 == 0 { (rnd() % 200000) as usize } else { (rnd() % 400) as usize };
            let sparse = it % 7 == 3;
            let d: Vec<u8> = (0..n).map(|_| { let c = alphabet[(rnd() % alphabet.len() as u64) as usize]; if sparse && c == b'\n' && rnd() % 50 != 0 { b'a' } else { c } }).collect();
            let mut got = Vec::new();
            scan::<3>(&d, 0, d.len(), |f| got.push(f.iter().map(|&(s, e)| d[s..e].to_vec()).collect::<Vec<_>>()));
            let mut want = Vec::new();
            let mut parts: Vec<&[u8]> = d.split(|&c| c == b'\n').collect();
            if d.last() == Some(&b'\n') { parts.pop(); }
            if d.is_empty() { parts.clear(); }
            for l in parts {
                let raw: Vec<&[u8]> = l.split(|&c| c == b',').collect();
                if raw.len() != 3 { continue; }
                let mut ok = true;
                let mut vals = Vec::new();
                for r in raw {
                    if r.contains(&b'"') {
                        if r.len() >= 2 && r[0] == b'"' && r[r.len()-1] == b'"' && !r[1..r.len()-1].contains(&b'"') { vals.push(r[1..r.len()-1].to_vec()); } else { ok = false; }
                    } else { vals.push(r.to_vec()); }
                }
                if ok { want.push(vals); }
            }
            assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&d));
        }
    }

#[test]
fn ingest_valid_f1() {
    let mt = Months::new(parse_ts_slow(b"2028-02-21T00:00:00Z").unwrap());
    let mut x: u64 = 77;
    let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
    for _ in 0..1_000_000 {
        let ms = 1_700_000_000_000i64 + (rnd() % 200_000_000_000) as i64;
        let secs = ms / 1000;
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        // civil from days
        let z = days + 719468;
        let era = z.div_euclid(146097);
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
        let s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, rem / 3600, rem % 3600 / 60, rem % 60);
        let mut buf = s.as_bytes().to_vec();
        buf.extend_from_slice(&[b'1'; 32]);
        assert_eq!(unsafe { parse_ingest(&buf[..20], &mt) }, parse_ts_slow(s.as_bytes()), "{s}");
    }
}
