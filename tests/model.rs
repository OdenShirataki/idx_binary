//! Compares IdxBinary against a simple model (row -> bytes map) under random
//! insert / update / delete operations with many duplicate values, and after
//! reopening the files.

use std::{collections::BTreeMap, num::NonZeroU32, path::Path};

use idx_binary::{AvltrieeSearch, AvltrieeUpdate, IdxBinary};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) % n as u64) as u32
    }
    fn value(&mut self) -> Vec<u8> {
        let n = self.next(40);
        // a few lengths and contents, so that values are often shared.
        format!("v{}{}", n, "x".repeat((n % 7) as usize)).into_bytes()
    }
}

fn check(idx: &IdxBinary, model: &BTreeMap<u32, Vec<u8>>) {
    for (&row, value) in model {
        assert_eq!(
            idx.value(NonZeroU32::new(row).unwrap()),
            Some(&value[..]),
            "row {row}"
        );
    }
    let rows: Vec<u32> = idx.as_ref().iter().map(|r| r.get()).collect();
    assert_eq!(rows.len(), model.len());
    for w in rows.windows(2) {
        assert_ne!(
            IdxBinary::cmp(&model[&w[0]], &model[&w[1]]),
            std::cmp::Ordering::Greater
        );
    }
}

fn run(dir: &Path, seed: u64) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let mut rng = Lcg(seed);
    let mut model: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    {
        let mut idx = IdxBinary::new(dir, 10);
        for _ in 0..1500 {
            let value = rng.value();
            match rng.next(10) {
                0..=4 => {
                    let row = idx.insert(&value);
                    model.insert(row.get(), value);
                }
                5 => {
                    let row = idx.row_or_insert(&value);
                    match model.get(&row.get()) {
                        Some(existing) => assert_eq!(existing, &value),
                        None => {
                            model.insert(row.get(), value);
                        }
                    }
                }
                6..=7 if !model.is_empty() => {
                    let keys: Vec<u32> = model.keys().copied().collect();
                    let row = keys[rng.next(keys.len() as u32) as usize];
                    idx.update(NonZeroU32::new(row).unwrap(), &value);
                    model.insert(row, value);
                }
                _ if !model.is_empty() => {
                    let keys: Vec<u32> = model.keys().copied().collect();
                    let row = keys[rng.next(keys.len() as u32) as usize];
                    idx.delete(NonZeroU32::new(row).unwrap());
                    model.remove(&row);
                }
                _ => {}
            }
            check(&idx, &model);
        }
    }

    let mut idx = IdxBinary::new(dir, 10);
    check(&idx, &model);
    for _ in 0..300 {
        let value = rng.value();
        let row = idx.insert(&value);
        assert!(model.insert(row.get(), value).is_none(), "row {row} reused");
        check(&idx, &model);
    }
}

#[test]
fn random_operations_match_model() {
    let base = std::env::temp_dir().join("idx_binary-model-test");
    for seed in 0..5 {
        run(&base.join(seed.to_string()), seed);
    }
}
