use std::{
    fs,
    io::{self, Write},
    time::{Instant, Duration},
    hash::Hasher,
    collections::HashSet,
    path::Path,
    hint::black_box,
};
use rand::{
    Rng, SeedableRng,
    distributions::Alphanumeric,
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

// #[inline]
// fn generate_bytes(rng: &mut impl Rng) -> impl Iterator<Item = u8> + '_ {
//     Standard.sample_iter(rng).flat_map(|x: u64| x.to_ne_bytes())
// }

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
    eprintln!("    -> {:5.0}±{:5.0} Mb/s", mean, sd);
    writeln!(writer, "{}\t{}\t{}\t{}\t{:.10}\t{:.10}", name, bytes, count, iters, mean, sd)?;
    Ok(())
}

/// Fills iterator with the number in HEX format.
#[inline]
fn fill_hex<'a>(rev_iter: impl Iterator<Item = &'a mut u8>, mut val: u64) {
    const LETTERS: [u8; 16] = *b"0123456789ABCDEF";
    for byte in rev_iter {
        *byte = LETTERS[(val & 0xf) as usize];
        val >>= 4;
    }
    assert!(val == 0);
}

/// Check collisions on `count` strings with variable infix at `affix_range` and
/// identical remaining alphanumeric string.
fn test_collisions<H>(
    name: &str,
    rng: &mut impl Rng,
    count: usize,
    length: usize,
    affix_range: std::ops::Range<usize>,
    writer: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    eprintln!("Testing {} for collisions, {}-string with variable range {:?}", name, length, affix_range);
    let timer = Instant::now();
    let mut buffer: Vec<_> = (0..length).map(|_| rng.sample(Alphanumeric)).collect();

    let mut collisions = 0;
    let mut set: HashSet<u64, ahash::RandomState> = Default::default();
    for val in 0..count as u64 {
        fill_hex(buffer[affix_range.clone()].iter_mut().rev(), val);
        let mut hasher = H::default();
        hasher.write(&buffer);
        collisions += u64::from(!set.insert(hasher.finish()));
    }
    writeln!(writer, "{}\t{}\t{}\t{}\t{}\t{}", name, length, affix_range.start, affix_range.end,
        collisions, count)?;
    eprintln!("    -> {:.2} s, {} collisions / {}", timer.elapsed().as_secs_f64(), collisions, count);
    Ok(())
}

fn test_hasher<H>(
    name: &str,
    mut rng: impl Rng,
    writer1: Option<&mut io::BufWriter<fs::File>>,
    writer2: Option<&mut io::BufWriter<fs::File>>,
) -> io::Result<()>
where H: Hasher + Default,
{
    if let Some(writer1) = writer1 {
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
    }

    if let Some(writer2) = writer2 {
        let count = 2_usize.pow(22);
        let affix = 6;
        for &size in &[16, 20, 24, 28] {
            test_collisions::<H>(name, &mut rng, count, size, 0..affix, writer2)?;
            test_collisions::<H>(name, &mut rng, count, size, 8..8 + affix, writer2)?;
            test_collisions::<H>(name, &mut rng, count, size, size - affix..size, writer2)?;
        }
    }
    eprintln!();
    Ok(())
}

fn main() {
    let out_dir = Path::new("out");
    if !out_dir.exists() {
        fs::create_dir(out_dir).unwrap();
    }

    let calc_bandwidth = true;
    let calc_collisions = true;

    let mut writer1 = if calc_bandwidth {
        let mut writer = io::BufWriter::new(fs::File::create(out_dir.join("bandwidth.csv")).unwrap());
        writeln!(writer, "hasher\tbytes\tcount\titers\tbandwidth_mean\tbandwidth_sd").unwrap();
        Some(writer)
    } else {
        None
    };
    let mut writer2 = if calc_collisions {
        let mut writer = io::BufWriter::new(fs::File::create(out_dir.join("collisions.csv")).unwrap());
        writeln!(writer, "hasher\tlength\tvar_start\tvar_end\tcollisions\tsize").unwrap();
        Some(writer)
    } else {
        None
    };

    let rng = rand_xoshiro::Xoshiro256PlusPlus::from_entropy();
    test_hasher::<siphasher::sip::SipHasher13>("sip13", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<siphasher::sip::SipHasher24>("sip24", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<ahash::AHasher>("ahash", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<seahash::SeaHasher>("seahash", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<metrohash::MetroHash64>("metro64", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<metrohash::MetroHash128>("metro128", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<rustc_hash::FxHasher>("fxhash", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<wyhash::WyHash>("wyhash", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<wyhash2::WyHash>("wyhash2", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<xxhash_rust::xxh64::Xxh64>("xxhash64", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<highway::HighwayHasher>("highway", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<fasthash::T1haHasher>("t1ha", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
    test_hasher::<fnv::FnvHasher>("fnv", rng.clone(), writer1.as_mut(), writer2.as_mut()).unwrap();
}
