use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    num::NonZeroU32,
    path::Path,
};

pub use idx_file::{
    AvltrieeIter, AvltrieeSearch, AvltrieeUpdate, FileMmap, IdxFile, IdxFileAvlTriee,
};

use idx_file::{AvltrieeNode, IdxFileAllocator};
use various_data_file::{DataAddress, VariousDataFile};

type IdxBinaryAvltriee = IdxFileAvlTriee<DataAddress, [u8]>;
type IdxBinaryAllocator = IdxFileAllocator<DataAddress>;

pub struct IdxBinary {
    index: IdxFile<DataAddress, [u8]>,
    data_file: VariousDataFile,
}

impl AsRef<IdxBinaryAvltriee> for IdxBinary {
    fn as_ref(&self) -> &IdxBinaryAvltriee {
        &self.index
    }
}

impl AsMut<IdxBinaryAvltriee> for IdxBinary {
    fn as_mut(&mut self) -> &mut IdxBinaryAvltriee {
        &mut self.index
    }
}

impl AvltrieeSearch<DataAddress, [u8], IdxBinaryAllocator> for IdxBinary {
    fn cmp(left: &[u8], right: &[u8]) -> Ordering {
        let mut left = left.into_iter().fuse();
        let mut right = right.into_iter().fuse();

        let mut l;
        let mut r;
        let mut ll;
        let mut rr;

        macro_rules! to_digit {
            ($v:expr) => {
                $v.and_then(|v| {
                    let v = *v as isize;
                    (v >= ('0' as isize) && v <= ('9' as isize)).then_some(v - 48)
                })
            };
        }

        macro_rules! read_left {
            () => {{
                l = left.next();
                ll = to_digit!(l);
            }};
        }

        macro_rules! read_right {
            () => {{
                r = right.next();
                rr = to_digit!(r);
            }};
        }

        macro_rules! return_unless_equal {
            ($ord:expr) => {
                match $ord {
                    Equal => {}
                    lastcmp => return lastcmp,
                }
            };
        }

        read_left!();
        read_right!();
        'nondigits: loop {
            match (l, r) {
                (Some(l_), Some(r_)) => match (ll, rr) {
                    (Some(ll_), Some(rr_)) => {
                        if ll_ == 0 || rr_ == 0 {
                            // left-aligned matching. (`015` < `12`)
                            return_unless_equal!(ll_.cmp(&rr_));
                            'digits_left: loop {
                                read_left!();
                                read_right!();
                                match (ll, rr) {
                                    (Some(ll_), Some(rr_)) => return_unless_equal!(ll_.cmp(&rr_)),
                                    (Some(_), None) => return Greater,
                                    (None, Some(_)) => return Less,
                                    (None, None) => break 'digits_left,
                                }
                            }
                        } else {
                            // right-aligned matching. (`15` < `123`)
                            let mut lastcmp = ll_.cmp(&rr_);
                            'digits_right: loop {
                                read_left!();
                                read_right!();
                                match (ll, rr) {
                                    (Some(ll_), Some(rr_)) => {
                                        // `lastcmp` is only used when there are the same number of
                                        // digits, so we only update it.
                                        if lastcmp == Equal {
                                            lastcmp = ll_.cmp(&rr_);
                                        }
                                    }
                                    (Some(_), None) => return Greater,
                                    (None, Some(_)) => return Less,
                                    (None, None) => break 'digits_right,
                                }
                            }
                            return_unless_equal!(lastcmp);
                        }
                        continue 'nondigits; // do not read from the iterators again
                    }
                    (_, _) => return_unless_equal!(l_.cmp(r_)),
                },
                (Some(_), None) => return Greater,
                (None, Some(_)) => return Less,
                (None, None) => return Equal,
            }
            read_left!();
            read_right!();
        }
    }

    /// Returns the value of the specified row. Returns None if the row does not exist.
    fn value(&self, row: NonZeroU32) -> Option<&[u8]> {
        self.as_ref().node(row).map(|v| self.data_file.bytes(v))
    }

    /// Returns the value of the specified row.
    unsafe fn value_unchecked(&self, row: NonZeroU32) -> &[u8] {
        self.data_file.bytes(self.as_ref().node_unchecked(row))
    }

    /// Returns node and value of the specified row.
    unsafe fn node_value_unchecked(&self, row: NonZeroU32) -> (&AvltrieeNode<DataAddress>, &[u8]) {
        let node = self.as_ref().node_unchecked(row);
        (node, self.data_file.bytes(node))
    }
}

impl AvltrieeUpdate<DataAddress, [u8], IdxBinaryAllocator> for IdxBinary {
    fn convert_on_insert_unique(&mut self, input: &[u8]) -> DataAddress {
        self.data_file.insert(input).into_address()
    }

    fn on_delete(&mut self, row: NonZeroU32) {
        if let Some((true, node)) = self.index.is_unique(row) {
            self.data_file.delete((**node).clone());
        }
    }
}

impl IdxBinary {
    /// Opens the file and creates the IdxBinary.
    /// # Arguments
    /// * `path` - Path of directory to save data.
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(directory: P, allocation_lot: u32) -> Self {
        let path = directory.as_ref();
        Self {
            index: IdxFile::new(
                {
                    let mut path = path.to_path_buf();
                    path.push(".i");
                    path
                },
                allocation_lot,
            ),
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.push(".d");
                path
            }),
        }
    }

    /// Opens the file and creates the IdxBinary.
    /// # Arguments
    /// * `path` - Path of part of filename without extension to save data.
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new_ext<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let path = path.as_ref();
        Self {
            index: IdxFile::new(path.with_extension("i"), allocation_lot),
            data_file: VariousDataFile::new(path.with_extension("d")),
        }
    }

    /// Finds a sequence of bytes, inserts it if it doesn't exist, and returns a row.
    pub fn row_or_insert(&mut self, content: &[u8]) -> NonZeroU32 {
        let edge = self.edge(content);
        if let (Some(row), Ordering::Equal) = edge {
            row
        } else {
            let row = unsafe { NonZeroU32::new_unchecked(self.index.rows_count() + 1) };
            unsafe {
                self.index.insert_unique_unchecked(
                    row,
                    self.data_file.insert(content).into_address(),
                    edge,
                );
            }
            row
        }
    }
}
