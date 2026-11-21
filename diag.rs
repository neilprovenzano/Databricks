// DIAGNOSTIC BUILD. On the scorer's dev-set check this prints machine details and
// benchmarks to stderr, times the real pipeline on a 20x scaled copy of the dev data,
// and then writes deliberately empty output so the check fails and shows these logs.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn read(p: &str) -> String {
    std::fs::read_to_string(p).unwrap_or_else(|e| format!("<{e}>"))
}

fn first_line_with(p: &str, key: &str) -> String {
    read(p).lines().find(|l| l.starts_with(key)).unwrap_or("?").to_string()
}

fn info() {
    eprintln!("==== MACHINE");
    eprintln!("cpu: {}", first_line_with("/proc/cpuinfo", "model name"));
    eprintln!("flags avx512bw: {}", read("/proc/cpuinfo").contains("avx512bw"));
    eprintln!("nproc (available_parallelism): {:?}", std::thread::available_parallelism());
    let online = read("/sys/devices/system/cpu/online");
    eprintln!("cpus online: {}", online.trim());
    for i in 0..4 {
        let base = format!("/sys/devices/system/cpu/cpu0/cache/index{i}");
        eprintln!("cache L{} {}: {}", read(&format!("{base}/level")).trim(), read(&format!("{base}/type")).trim(), read(&format!("{base}/size")).trim());
    }
    eprintln!("cpu0 siblings: {}", read("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list").trim());
    eprintln!("thp enabled: {}", read("/sys/kernel/mm/transparent_hugepage/enabled").trim());
    eprintln!("thp defrag: {}", read("/sys/kernel/mm/transparent_hugepage/defrag").trim());
    eprintln!("kernel: {}", read("/proc/version").trim());
    eprintln!("{}", first_line_with("/proc/meminfo", "MemTotal"));
    eprintln!("cgroup cpu.max: {}", read("/sys/fs/cgroup/cpu.max").trim());
    eprintln!("cgroup cpuset: {}", read("/sys/fs/cgroup/cpuset.cpus.effective").trim());
    eprintln!("cgroup memory.max: {}", read("/sys/fs/cgroup/memory.max").trim());
    for l in read("/proc/self/mountinfo").lines() {
        if l.contains(" /data ") || l.contains(" /scratch ") || l.contains(" /output ") {
            eprintln!("mount: {l}");
        }
    }
    let up = read("/proc/uptime");
    eprintln!("uptime: {}", up.trim());
    eprintln!("process start to diag: {:?}", std::time::SystemTime::now());
}

fn par<F: Fn(usize) + Sync>(n: usize, f: F) {
    std::thread::scope(|s| {
        let f = &f;
        for t in 0..n {
            s.spawn(move || f(t));
        }
    });
}

fn mem_bench() {
    eprintln!("==== MEMORY");
    let n = 1usize << 30; // 1 GiB
    let t = Instant::now();
    let mut v: Vec<u64> = Vec::with_capacity(n / 8);
    unsafe { v.set_len(n / 8) };
    // first touch with 8 threads (page faults + zeroing)
    let p = v.as_mut_ptr() as usize;
    par(8, |th| {
        let per = n / 8 / 8;
        let base = p as *mut u64;
        for i in (th * per..(th + 1) * per).step_by(512) {
            unsafe { *base.add(i) = 1 };
        }
    });
    eprintln!("first touch 1 GiB (8 threads, huge-page advised): {:.3}s", t.elapsed().as_secs_f64());
    for &th in &[1usize, 2, 4, 8] {
        let t = Instant::now();
        let sums: Vec<std::sync::atomic::AtomicU64> = (0..th).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
        par(th, |k| {
            let per = n / 8 / th;
            let base = p as *const u64;
            let mut a = 0u64;
            for i in k * per..(k + 1) * per {
                a = a.wrapping_add(unsafe { *base.add(i) });
            }
            sums[k].store(a, std::sync::atomic::Ordering::Relaxed);
        });
        let el = t.elapsed().as_secs_f64();
        eprintln!("read 1 GiB with {th} threads: {:.3}s = {:.1} GB/s", el, 1.0737 / el);
    }
    for &th in &[1usize, 4, 8] {
        let t = Instant::now();
        par(th, |k| {
            let per = n / 8 / th;
            let base = p as *mut u64;
            for i in k * per..(k + 1) * per {
                unsafe { *base.add(i) = i as u64 };
            }
        });
        let el = t.elapsed().as_secs_f64();
        eprintln!("write 1 GiB with {th} threads: {:.3}s = {:.1} GB/s", el, 1.0737 / el);
    }
    // compute-only scaling (no memory): a dependent multiply chain per thread
    for &th in &[1usize, 4, 8] {
        let t = Instant::now();
        par(th, |k| {
            let mut x = k as u64 + 1;
            for _ in 0..200_000_000u64 {
                x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407));
            }
            std::hint::black_box(x);
        });
        eprintln!("compute 200M steps per thread, {th} threads: {:.3}s", t.elapsed().as_secs_f64());
    }
    drop(v);
}

fn csvs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir).map(|r| r.flatten().map(|e| e.path()).filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false)).collect()).unwrap_or_default();
    v.sort();
    v
}

/// Append "_k" to a raw field (before a closing quote if quoted); empty stays empty.
fn suffix(out: &mut Vec<u8>, f: &[u8], k: usize) {
    if f.is_empty() {
        return;
    }
    let tag = format!("_{k}");
    if f.len() >= 2 && f[f.len() - 1] == b'"' {
        out.extend_from_slice(&f[..f.len() - 1]);
        out.extend_from_slice(tag.as_bytes());
        out.push(b'"');
    } else {
        out.extend_from_slice(f);
        out.extend_from_slice(tag.as_bytes());
    }
}

/// Copy of a CSV file holding `reps` disjoint replicas: the first `nid` fields get a per-replica suffix.
fn replicate(src: &Path, dst: &Path, reps: usize, nid: usize) {
    let data = std::fs::read(src).unwrap();
    let mut lines: Vec<&[u8]> = data.split(|&c| c == b'\n').collect();
    if data.last() == Some(&b'\n') {
        lines.pop();
    }
    let mut out = Vec::with_capacity(data.len() * reps + 1024);
    if lines.is_empty() {
        std::fs::write(dst, &data).unwrap();
        return;
    }
    out.extend_from_slice(lines[0]);
    out.push(b'\n');
    for k in 0..reps {
        for l in &lines[1..] {
            // split off the first nid fields
            let mut rest: &[u8] = l;
            let mut ok = true;
            let mut parts: Vec<&[u8]> = Vec::new();
            for _ in 0..nid {
                match rest.iter().position(|&c| c == b',') {
                    Some(i) => {
                        parts.push(&rest[..i]);
                        rest = &rest[i + 1..];
                    }
                    None => {
                        if parts.len() + 1 == nid {
                            parts.push(rest);
                            rest = &[];
                            break;
                        }
                        ok = false;
                        break;
                    }
                }
            }
            if !ok || parts.len() != nid {
                out.extend_from_slice(l); // malformed: keep as is
                out.push(b'\n');
                continue;
            }
            for (j, p) in parts.iter().enumerate() {
                if j > 0 {
                    out.push(b',');
                }
                suffix(&mut out, p, k);
            }
            if nid == 2 && rest.is_empty() && l.iter().filter(|&&c| c == b',').count() == 1 {
                // links line: exactly two fields
            } else {
                out.push(b',');
                out.extend_from_slice(rest);
            }
            out.push(b'\n');
        }
    }
    std::fs::write(dst, &out).unwrap();
}

fn build_big(src: &Path, dst: &Path, reps: usize) {
    let t = Instant::now();
    let _ = std::fs::remove_dir_all(dst);
    for d in ["events", "clicks", "links"] {
        std::fs::create_dir_all(dst.join(d)).unwrap();
    }
    for f in ["products.csv", "campaigns.csv", "manifest.json"] {
        std::fs::copy(src.join(f), dst.join(f)).unwrap();
    }
    let mut jobs: Vec<(PathBuf, PathBuf, usize)> = Vec::new();
    for (d, nid) in [("events", 2usize), ("clicks", 2), ("links", 2)] {
        for p in csvs(&src.join(d)) {
            jobs.push((p.clone(), dst.join(d).join(p.file_name().unwrap()), nid));
        }
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    par(8, |_| loop {
        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if i >= jobs.len() {
            break;
        }
        let (s, d, nid) = &jobs[i];
        replicate(s, d, reps, *nid);
    });
    let bytes: u64 = jobs.iter().map(|j| std::fs::metadata(&j.1).map(|m| m.len()).unwrap_or(0)).sum();
    eprintln!("built {reps}x dataset: {:.2} GB in {:.2}s", bytes as f64 / 1e9, t.elapsed().as_secs_f64());
}

fn pread_bench(dir: &Path) {
    let files: Vec<PathBuf> = csvs(&dir.join("events"));
    for &th in &[1usize, 4, 8] {
        let t = Instant::now();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let total = std::sync::atomic::AtomicU64::new(0);
        // chunks of 256 KB across all files
        let mut chunks: Vec<(usize, u64)> = Vec::new();
        for (i, f) in files.iter().enumerate() {
            let n = std::fs::metadata(f).unwrap().len();
            let mut o = 0;
            while o < n {
                chunks.push((i, o));
                o += 256 << 10;
            }
        }
        let handles: Vec<std::fs::File> = files.iter().map(|f| std::fs::File::open(f).unwrap()).collect();
        par(th, |_| {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; 256 << 10];
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= chunks.len() {
                    break;
                }
                let (fi, o) = chunks[i];
                let n = handles[fi].read_at(&mut buf, o).unwrap();
                total.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            }
        });
        let el = t.elapsed().as_secs_f64();
        let b = total.load(std::sync::atomic::Ordering::Relaxed) as f64;
        eprintln!("pread events {:.2} GB with {th} threads: {:.3}s = {:.1} GB/s", b / 1e9, el, b / el / 1e9);
    }
}

fn child(data: &Path, out: &Path, ncpu: usize, label: &str) {
    let _ = std::fs::remove_dir_all(out);
    std::fs::create_dir_all(out).unwrap();
    let exe = std::env::current_exe().unwrap();
    let t = Instant::now();
    let st = std::process::Command::new(exe)
        .env("DIAG_CHILD", "1")
        .env("VERBOSE", "1")
        .env("DATA_DIR", data)
        .env("OUTPUT_DIR", out)
        .env("NCPU", ncpu.to_string())
        .status();
    eprintln!(">>> {label}: NCPU={ncpu} wall {:.3}s status {:?}", t.elapsed().as_secs_f64(), st.map(|s| s.code()));
}

pub fn run() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let scratch = PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into()));
    eprintln!("######## DIAGNOSTIC BUILD (output is intentionally empty) ########");
    info();
    // real pipeline on the dev data, cold-ish then warm
    eprintln!("==== PIPELINE ON /data (dev set)");
    child(&data, &scratch.join("o_dev1"), 8, "dev run 1");
    child(&data, &scratch.join("o_dev2"), 8, "dev run 2");
    mem_bench();
    eprintln!("==== BUILD 20x DATASET IN /scratch");
    let big = scratch.join("big20");
    build_big(&data, &big, 20);
    pread_bench(&big);
    eprintln!("==== PIPELINE ON 20x DATASET");
    for &n in &[8usize, 8, 4, 2, 8] {
        child(&big, &scratch.join("o_big"), n, "20x");
    }
    let _ = std::fs::remove_dir_all(&big);
    let _ = std::fs::remove_dir_all(scratch.join("o_big"));
    // deliberately empty output so the dev check fails and shows these logs
    for t in ["user_daily", "campaign_daily"] {
        let _ = std::fs::create_dir_all(out.join(t));
    }
    crate::out::write_empty(&out.join("user_daily").join("part-000.parquet"), &out.join("campaign_daily").join("part-000.parquet")).unwrap();
    eprintln!("######## DIAGNOSTIC DONE in {:.2}s ########", t0.elapsed().as_secs_f64());
    let _ = std::io::stderr().flush();
}
