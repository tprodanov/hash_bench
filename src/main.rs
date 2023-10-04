use std::{
    fs,
    io::{self, Write},
    time::Instant,
    hash::Hasher,
    path::Path,
    hint::black_box,
};
use rand::{
    Rng, SeedableRng,
    distributions::{Alphanumeric, Standard, Distribution},
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

#[inline]
fn generate_bytes(rng: &mut impl Rng) -> impl Iterator<Item = u8> + '_ {
    Standard.sample_iter(rng).flat_map(|x: u64| x.to_ne_bytes())
}

#[inline]
fn calc<H: Hasher + Default>(bytes: &[u8]) -> u64 {
    let mut hasher = H::default();
    hasher.write(bytes);
    hasher.finish()
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
        let timer = Instant::now();
        for _ in 0..count {
            black_box(calc::<H>(black_box(&buffer)));
        }
        let runtime = timer.elapsed();
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
    assert!(count <= 16_usize.pow(affix_range.len() as u32));

    let mut collisions = 0;
    let mut set: std::collections::HashSet<u64, ahash::RandomState> = Default::default();
    for val in 0..count as u64 {
        fill_hex(buffer[affix_range.clone()].iter_mut().rev(), val);
        collisions += u64::from(!set.insert(calc::<H>(&buffer)));
    }
    writeln!(writer, "{}\t{}\t{}\t{}\t{}\t{}", name, length, affix_range.start, affix_range.end,
        collisions, count)?;
    eprintln!("    -> {:.2} s, {} collisions / {}", timer.elapsed().as_secs_f64(), collisions, count);
    Ok(())
}

fn test_randomness<H>(
    name: &str,
    rng: &mut impl Rng,
    count: usize,
    length: usize,
    writer: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    eprintln!("Testing {} for randomness, length {}", name, length);
    let timer = Instant::now();
    let mut buffer = vec![0; length];
    let mut bytes = generate_bytes(rng);
    let mut matches_count = [0_u64; 65];
    for _ in 0..count {
        buffer.iter_mut().for_each(|b| *b = bytes.next().unwrap());
        let hash0 = calc::<H>(&buffer);
        for i in 0..length {
            let b = *unsafe { buffer.get_unchecked(i) };
            unsafe { *buffer.get_unchecked_mut(i) = b.wrapping_add(1) };
            let hash = calc::<H>(&buffer);
            unsafe { *buffer.get_unchecked_mut(i) = b };
            matches_count[(hash0 ^ hash).count_ones() as usize] += 1;
        }
    }
    let average_change = matches_count.into_iter().enumerate()
        .map(|(i, c)| (i as u64 * c) as f64)
        .sum::<f64>()
        / (length * count) as f64;
    let randomness01 = 1.0 - (average_change / 32.0 - 1.0).abs();
    writeln!(writer, "{}\t{}\t{:.7}\t{:.10}", name, length, average_change, randomness01)?;
    eprintln!("    -> {:.2} s, {:.3} bits changed on average, randomness {:.5}", timer.elapsed().as_secs_f64(),
        average_change, randomness01);
    Ok(())
}

fn test_hasher<H>(
    name: &str,
    mut rng: impl Rng,
    writer1: Option<&mut io::BufWriter<fs::File>>,
    writer2: Option<&mut io::BufWriter<fs::File>>,
    writer3: Option<&mut io::BufWriter<fs::File>>,
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
        evaluate::<H>(name, 128, 2_usize.pow(16), ITERS, writer1)?;
        evaluate::<H>(name, 256, 2_usize.pow(15), ITERS, writer1)?;
        evaluate::<H>(name, 512, 2_usize.pow(15), ITERS, writer1)?;
        evaluate::<H>(name, 1024, 2_usize.pow(14), ITERS, writer1)?;
        evaluate::<H>(name, 2048, 2_usize.pow(14), ITERS, writer1)?;
        evaluate::<H>(name, 4096, 2_usize.pow(14), ITERS, writer1)?;
    }

    if let Some(writer2) = writer2 {
        let count = 2_usize.pow(24);
        let affix = 6;
        for size in (8..=32).step_by(2) {
            // test_collisions::<H>(name, &mut rng, count, size, 0..affix, writer2)?;
            // test_collisions::<H>(name, &mut rng, count, size, 8..8 + affix, writer2)?;
            test_collisions::<H>(name, &mut rng, count, size + affix, size..size + affix, writer2)?;
        }
    }

    if let Some(writer3) = writer3 {
        let count = 2_usize.pow(22);
        for &size in &[8, 12, 16, 20, 24, 28, 32] {
            test_randomness::<H>(name, &mut rng, count, size, writer3)?;
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
    let calc_randomness = true;

    let mut writer1 = if calc_bandwidth {
        let mut writer = io::BufWriter::new(fs::File::create(out_dir.join("bandwidth.csv")).unwrap());
        writeln!(writer, "hasher\tbytes\tcount\titers\tbandwidth_mean\tbandwidth_sd").unwrap();
        Some(writer)
    } else {
        None
    };
    let mut writer2 = if calc_collisions {
        let mut writer = io::BufWriter::new(fs::File::create(out_dir.join("collisions.csv")).unwrap());
        writeln!(writer, "hasher\tbytes\tvar_start\tvar_end\tcollisions\tcount").unwrap();
        Some(writer)
    } else {
        None
    };
    let mut writer3 = if calc_randomness {
        let mut writer = io::BufWriter::new(fs::File::create(out_dir.join("randomness.csv")).unwrap());
        writeln!(writer, "hasher\tbytes\tchanged_bits\trandomness").unwrap();
        Some(writer)
    } else {
        None
    };

    let rng = rand_xoshiro::Xoshiro256PlusPlus::from_entropy();
    test_hasher::<siphasher::sip::SipHasher13>("sip13", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<siphasher::sip::SipHasher24>("sip24", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<ahash::AHasher>("ahash", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<seahash::SeaHasher>("seahash", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<metrohash::MetroHash64>("metro64", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<metrohash::MetroHash128>("metro128", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<rustc_hash::FxHasher>("fxhash", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<wyhash::WyHash>("wyhash", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<wyhash2::WyHash>("wyhash2", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<xxhash_rust::xxh64::Xxh64>("xxhash64", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<highway::HighwayHasher>("highway", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::T1haHasher>("t1ha", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fnv::FnvHasher>("fnv", rng.clone(),
        writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::murmur2::Hasher64_x64>("murmur2",
        rng.clone(), writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::murmur3::Hasher128_x64>("murmur3",
            rng.clone(), writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::CityHasher>("city",
        rng.clone(), writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::SpookyHasher>("spooky",
        rng.clone(), writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
    test_hasher::<fasthash::FarmHasher>("farm",
        rng.clone(), writer1.as_mut(), writer2.as_mut(), writer3.as_mut()).unwrap();
}
