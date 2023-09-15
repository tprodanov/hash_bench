use std::{
    fs,
    io::{self, Write},
    time::{Instant, Duration},
    hash::Hasher,
    collections::HashSet,
    path::Path,
    hint::black_box,
};

/// Returns mean and variance together.
pub fn mean_variance(a: &[f64]) -> (f64, f64) {
    let n = a.len();
    assert!(n > 1);
    let mean = a.iter().sum::<f64>() / n as f64;
    let var = a.iter().fold(0.0, |acc, x| {
            let diff = x - mean;
            acc + diff * diff
        }) / (n - 1) as f64;
    (mean, var)
}

fn run_hasher<H>(buffer: &[u8], count: usize) -> Duration
where H: Hasher + Default,
{
    let timer = Instant::now();
    for _ in 0..count {
        let mut hasher = H::default();
        hasher.write(black_box(buffer));
        black_box(hasher.finish());
    }
    timer.elapsed()
}

fn evaluate<H>(
    name: &str,
    bytes: usize,
    count: usize,
    iters: usize,
    writer: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    eprintln!("Running {} on {} bytes", name, bytes);
    let buffer = vec![15; bytes];
    let mut values = Vec::with_capacity(iters);
    for _ in 0..iters {
        let runtime = run_hasher::<H>(&buffer, count);
        let bandwidth = 1e-6 * (count * bytes) as f64 / runtime.as_secs_f64();
        values.push(bandwidth);
    }
    let (mean, var) = mean_variance(&values);
    let sd = var.sqrt();
    eprintln!("    -> {:6.1} ± {:6.1} Mb/s", mean, sd);
    writeln!(writer, "{}\t{}\t{}\t{}\t{:.10}\t{:.10}", name, bytes, count, iters, mean, sd)?;
    Ok(())
}

fn test_collisions<H>(
    name: &str,
    start: u64,
    count: usize,
    step: usize,
    writer: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    let timer = Instant::now();
    eprintln!("Testing {} for collisions", name);
    let mut val = start;
    let mut collisions = 0;
    let mut set: HashSet<u64, ahash::RandomState> = Default::default();
    for i in 1..=count {
        let mut hasher = H::default();
        hasher.write(format!("{:016X}", val).as_bytes());
        collisions += u64::from(!set.insert(hasher.finish()));
        val = val.wrapping_add(1);
        if i % step == 0 {
            writeln!(writer, "{}\t{}\t{}", name, collisions, i)?;
        }
    }
    if count % step != 0 {
        writeln!(writer, "{}\t{}\t{}", name, collisions, count)?;
    }
    eprintln!("    -> {:.6} s, {} collisions", timer.elapsed().as_secs_f64(), collisions);
    Ok(())
}

fn test_hasher<H>(
    name: &str,
    writer1: &mut impl Write,
    writer2: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    const ITERS: usize = 1024;
    evaluate::<H>(name, 4, 2_usize.pow(18), ITERS, writer1)?;
    evaluate::<H>(name, 8, 2_usize.pow(18), ITERS, writer1)?;
    evaluate::<H>(name, 12, 2_usize.pow(18), ITERS, writer1)?;
    evaluate::<H>(name, 16, 2_usize.pow(18), ITERS, writer1)?;
    evaluate::<H>(name, 32, 2_usize.pow(17), ITERS, writer1)?;
    evaluate::<H>(name, 64, 2_usize.pow(16), ITERS, writer1)?;
    evaluate::<H>(name, 128, 2_usize.pow(15), ITERS, writer1)?;
    evaluate::<H>(name, 512, 2_usize.pow(14), ITERS, writer1)?;
    evaluate::<H>(name, 1024, 2_usize.pow(14), ITERS, writer1)?;

    let start = 1024_u64;
    test_collisions::<H>(name, start, 2_usize.pow(26), 2_usize.pow(20), writer2)?;
    eprintln!();
    Ok(())
}

// struct MyWyhash {
//     val: u64,
// }

// impl Default for MyWyhash {
//     fn default() -> MyWyhash {
//         MyWyhash { val: 0 }
//     }
// }

// impl Hasher for MyWyhash {
//     fn write(&mut self, buf: &[u8]) {
//         self.val = wyhash2::wyhash_single(buf, 0);
//     }

//     fn finish(&self) -> u64 {
//         self.val
//     }
// }

fn main() {
    let out_dir = Path::new("out");
    if !out_dir.exists() {
        fs::create_dir(out_dir).unwrap();
    }

    let mut writer1 = io::BufWriter::new(fs::File::create(out_dir.join("bandwidth.csv")).unwrap());
    writeln!(writer1, "hasher\tbytes\tcount\titers\tbandwidth_mean\tbandwidth_sd").unwrap();
    let mut writer2 = io::BufWriter::new(fs::File::create(out_dir.join("collisions.csv")).unwrap());
    writeln!(writer2, "hasher\tcollisions\tsize").unwrap();

    test_hasher::<siphasher::sip::SipHasher13>("sip13", &mut writer1, &mut writer2).unwrap();
    test_hasher::<siphasher::sip::SipHasher24>("sip24", &mut writer1, &mut writer2).unwrap();
    test_hasher::<ahash::AHasher>("ahash", &mut writer1, &mut writer2).unwrap();
    test_hasher::<seahash::SeaHasher>("seahash", &mut writer1, &mut writer2).unwrap();
    test_hasher::<metrohash::MetroHash64>("metro64", &mut writer1, &mut writer2).unwrap();
    test_hasher::<metrohash::MetroHash128>("metro128", &mut writer1, &mut writer2).unwrap();
    test_hasher::<rustc_hash::FxHasher>("fxhash", &mut writer1, &mut writer2).unwrap();
    test_hasher::<wyhash::WyHash>("wyhash", &mut writer1, &mut writer2).unwrap();
    test_hasher::<wyhash2::WyHash>("wyhash2", &mut writer1, &mut writer2).unwrap();
    test_hasher::<xxhash_rust::xxh64::Xxh64>("xxhash64", &mut writer1, &mut writer2).unwrap();
    test_hasher::<highway::HighwayHasher>("highway", &mut writer1, &mut writer2).unwrap();
    test_hasher::<fasthash::T1haHasher>("t1ha", &mut writer1, &mut writer2).unwrap();
    test_hasher::<fnv::FnvHasher>("fnv", &mut writer1, &mut writer2).unwrap();
    // test_hasher::<adler::Adler32>("adler32", &mut writer1, &mut writer2).unwrap();
}
