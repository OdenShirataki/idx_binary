//! Checks that IdxBinary::cmp is a total order, which the tree relies on.

use std::cmp::Ordering;

use idx_binary::{AvltrieeSearch, IdxBinary};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) % n as u64) as u32
    }
    fn bytes(&mut self) -> Vec<u8> {
        const CHARS: &[u8] = b"0012a9b";
        (0..self.next(5))
            .map(|_| CHARS[self.next(CHARS.len() as u32) as usize])
            .collect()
    }
}

fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    IdxBinary::cmp(a, b)
}

fn show(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

#[test]
fn equal_only_for_identical_bytes() {
    let mut rng = Lcg(1);
    for _ in 0..200_000 {
        let (a, b) = (rng.bytes(), rng.bytes());
        assert_eq!(
            cmp(&a, &b) == Ordering::Equal,
            a == b,
            "{:?} vs {:?}",
            show(&a),
            show(&b)
        );
    }
}

#[test]
fn antisymmetric() {
    let mut rng = Lcg(2);
    for _ in 0..200_000 {
        let (a, b) = (rng.bytes(), rng.bytes());
        assert_eq!(
            cmp(&a, &b),
            cmp(&b, &a).reverse(),
            "{:?} vs {:?}",
            show(&a),
            show(&b)
        );
    }
}

#[test]
fn transitive() {
    let mut rng = Lcg(3);
    let mut values: Vec<Vec<u8>> = (0..300).map(|_| rng.bytes()).collect();
    values.sort_by(|a, b| cmp(a, b));
    values.dedup();
    for i in 0..values.len() {
        for j in i + 1..values.len() {
            assert_eq!(
                cmp(&values[i], &values[j]),
                Ordering::Less,
                "{:?} vs {:?}",
                show(&values[i]),
                show(&values[j])
            );
        }
    }
}
