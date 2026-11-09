# idx_binary

Indexed binary file. It uses mmap, various_data_file and avltriee.

Values are byte strings, ordered so that digit sequences compare as numbers
(for example `2` < `10`). The same value is stored only once.

## Example

```rust
use idx_binary::{AvltrieeSearch, AvltrieeUpdate, IdxBinary};

let dir = std::env::temp_dir().join("idx_binary-readme");
let _ = std::fs::remove_dir_all(&dir);
std::fs::create_dir_all(&dir).unwrap();

// The second argument is the number of rows to extend the file by at once.
let mut idx = IdxBinary::new(&dir, 100);

let item10 = idx.insert(b"item10");
let item2 = idx.insert(b"item2");
let apple = idx.insert(b"apple");

assert_eq!(idx.value(item10), Some(&b"item10"[..]));

// rows are iterated in value order, comparing digits as numbers.
let rows: Vec<_> = idx.as_ref().iter().collect();
assert_eq!(rows, [apple, item2, item10]);

// row_or_insert returns the existing row for a value that is already stored.
assert_eq!(idx.row_or_insert(b"apple"), apple);

idx.update(item2, b"item3");
idx.delete(apple);
assert_eq!(idx.row(b"item3"), Some(item2));
assert_eq!(idx.row(b"apple"), None);
```
