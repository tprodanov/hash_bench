mod gen;

use std::{
    fs,
    io::{self, Write},
    time::{Instant, Duration},
    hash::Hasher,
    collections::HashSet,
    path::Path,
};
// #[allow(deprecated)]
// use std::hash::SipHasher;
use rand::{Rng, SeedableRng};

/// Calculates hash for each element in `data`, extends hashes and returns running time.
fn extend_hashes<H, const N: usize>(data: &[[u8; N]], hashes: &mut Vec<u64>) -> Duration
where H: Hasher + Default,
{
    let timer = Instant::now();
    for bytes in data.iter() {
        let mut hasher = H::default();
        hasher.write(bytes);
        hashes.push(hasher.finish());
    }
    timer.elapsed()
}

/// Counts the number of collisions.
fn count_collisions(hashes: &[u64], set: &mut HashSet<u64, ahash::RandomState>) -> u32 {
    hashes.iter().map(|&hash| u32::from(!set.insert(hash))).sum::<u32>()
}

fn fmt_duration(duration: Duration) -> String {
    format!("{}.{:09}", duration.as_secs(), duration.subsec_nanos())
}

fn evaluate<H, const N: usize>(
    hasher_name: &str,
    data: &[[u8; N]],
    data_name: &str,
    time_writer: &mut impl Write,
    collisions_writer: &mut impl Write,
) -> io::Result<()>
where H: Hasher + Default,
{
    const ITERS: usize = 32;

    eprintln!("Running {} on {}-{}", hasher_name, data_name, N);
    let prefix = format!("{}\t{}\t{}\t", hasher_name, data_name, N);
    let size = data.len();

    let mut hashes = Vec::with_capacity(size / ITERS);
    let mut collisions = 0;
    let mut curr_size = 0;
    let mut collisions_set = HashSet::default();
    let mut sum_runtime = 0.0;
    for (i, chunk) in data.chunks(size / ITERS).enumerate() {
        hashes.clear();
        let runtime = extend_hashes::<H, N>(&chunk, &mut hashes);
        writeln!(time_writer, "{}{}\t{}\t{}", prefix, i + 1, chunk.len(), fmt_duration(runtime))?;
        sum_runtime += runtime.as_secs_f64();
        collisions += count_collisions(&hashes, &mut collisions_set);
        curr_size += chunk.len();
        writeln!(collisions_writer, "{}{}\t{}\t{}", prefix, i + 1, collisions, curr_size)?;
    }
    eprintln!("    -> {:.6} s,   {} collisions", sum_runtime, collisions);
    Ok(())
}

// Generate 2^30 bytes.
const DATA_SIZE: usize = 30;

fn test_hasher<H>(
    hasher_name: &str,
    time_writer: &mut impl Write,
    collisions_writer: &mut impl Write,
    mut rng: impl Rng,
) -> io::Result<()>
where H: Hasher + Default,
{
    evaluate::<H, 4>(hasher_name, &gen::consec_u32s(DATA_SIZE), "consec", time_writer, collisions_writer)?;
    evaluate::<H, 8>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 12>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 16>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 32>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 64>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 128>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 256>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 512>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;
    evaluate::<H, 1024>(hasher_name, &gen::random(&mut rng, DATA_SIZE), "random",
        time_writer, collisions_writer)?;

    evaluate::<H, 8>(hasher_name, &gen::similar_strings(&mut rng, DATA_SIZE), "similar",
        time_writer, collisions_writer)?;
    evaluate::<H, 12>(hasher_name, &gen::similar_strings(&mut rng, DATA_SIZE), "similar",
        time_writer, collisions_writer)?;
    evaluate::<H, 16>(hasher_name, &gen::similar_strings(&mut rng, DATA_SIZE), "similar",
        time_writer, collisions_writer)?;
    evaluate::<H, 32>(hasher_name, &gen::similar_strings(&mut rng, DATA_SIZE), "similar",
        time_writer, collisions_writer)?;
    Ok(())
}

fn main() {
    let out_dir = Path::new("out");
    if !out_dir.exists() {
        fs::create_dir(out_dir).unwrap();
    }

    let mut time_writer = io::BufWriter::new(fs::File::create(out_dir.join("time.csv")).unwrap());
    let mut collisions_writer = io::BufWriter::new(fs::File::create(out_dir.join("collisions.csv")).unwrap());
    writeln!(time_writer, "hasher\tdata\tbytes\titer\tsize\ttime").unwrap();
    writeln!(collisions_writer, "hasher\tdata\tbytes\titer\tcollisions\tsize").unwrap();

    let rng = rand_xoshiro::Xoshiro256PlusPlus::from_entropy();
    test_hasher::<siphasher::sip::SipHasher13>("sip13",
        &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    test_hasher::<siphasher::sip::SipHasher24>("sip24",
        &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<ahash::AHasher>("ahash",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<seahash::SeaHasher>("seahash",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<metrohash::MetroHash64>("metro64",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<metrohash::MetroHash128>("metro128",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    test_hasher::<rustc_hash::FxHasher>("fxhash",
        &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<wyhash::WyHash>("wyhash",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<xxhash_rust::xxh64::Xxh64>("xxhash64",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<highway::HighwayHasher>("highway",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<fasthash::T1haHasher>("t1ha",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
    // test_hasher::<fnv::FnvHasher>("fnv",
    //     &mut time_writer, &mut collisions_writer, rng.clone()).unwrap();
}
