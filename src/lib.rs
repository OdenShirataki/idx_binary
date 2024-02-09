mod compare;

use std::{cmp::Ordering, num::NonZeroU32, path::Path};

pub use compare::compare;
pub use idx_file::{
    AvltrieeIter, AvltrieeSearch, AvltrieeUpdate, FileMmap, IdxFile, IdxFileAvlTriee,
};

use idx_file::IdxFileAllocator;
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
    fn cmp(&self, left: &DataAddress, right: &[u8]) -> Ordering {
        compare(self.data_file.bytes(left), right)
    }

    fn invert<'a, 'b: 'a>(&'a self, value: &'b DataAddress) -> &[u8] {
        self.data_file.bytes(value)
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
    /// /// # Arguments
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
