use rand::{
    Rng,
    distributions::{Standard, Distribution},
};

/// Returns the number of elements based on the total data size (in bytes) and element size (also in bytes).
fn get_size(data_size: usize, el_size: usize) -> usize {
    2_usize.checked_pow(data_size.try_into().unwrap()).unwrap().checked_div(el_size).unwrap()
}

// pub fn subsample_u32(rng: &mut impl Rng, data_size: usize) -> Vec<[u8; 4]> {
//     let count = get_size(data_size, 4);
//     let mut res = Vec::with_capacity(count);
//     let length = usize::try_from(u32::MAX).unwrap().checked_add(1).unwrap();
//     res.extend(rand::seq::index::sample(rng, length, count).iter()
//         .map(|x: usize| (x as u32).to_ne_bytes()));
//     res
// }

/// Just take consecutive u32s, as random generation produces many duplicate arrays,
/// while subsampling takes a long time.
pub fn consec_u32s(data_size: usize) -> Vec<[u8; 4]> {
    (0..get_size(data_size, 4)).map(|i| u32::try_from(i).unwrap().to_ne_bytes()).collect()
}

#[inline]
fn generate_bytes(rng: &mut impl Rng) -> impl Iterator<Item = u8> + '_ {
    Standard.sample_iter(rng).flat_map(|x: u64| x.to_ne_bytes())
}

/// Generate `count` random N-byte arrays.
/// Generates `2^(data_size - N)` elements.
pub fn random<const N: usize>(rng: &mut impl Rng, data_size: usize) -> Vec<[u8; N]> {
    let mut byte_gen = generate_bytes(rng);
    let count = get_size(data_size, N);
    (0..count).map(|_| {
        let mut arr = [0; N];
        arr.iter_mut().for_each(|x| *x = byte_gen.next().unwrap());
        arr
    }).collect()
}

const NLETTERS: usize = 64;
const LETTERS: &[u8; NLETTERS] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_.";

/// Generates random alphanumeric strings (with . and _) of size `N - 2`,
/// and adds all possible alphanumeric suffixes of 2 letters.
pub fn similar_strings<const N: usize>(rng: &mut impl Rng, data_size: usize) -> Vec<[u8; N]> {
    assert!(N > 2);
    let count = get_size(data_size, N);
    let mut byte_gen = generate_bytes(rng);
    let mut res = Vec::with_capacity(count);
    loop {
        let mut random_string = [0; N];
        for ch in random_string[..N - 2].iter_mut() {
            *ch = LETTERS[usize::from(byte_gen.next().unwrap()) % NLETTERS];
        }
        for &letter1 in LETTERS {
            random_string[N - 2] = letter1;
            for &letter2 in LETTERS {
                random_string[N - 1] = letter2;
                if res.len() == count {
                    return res;
                }
                res.push(random_string);
            }
        }
    }
}
