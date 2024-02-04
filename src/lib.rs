mod compare;

pub use compare::compare;
pub use idx_file::{Avltriee, AvltrieeIter, AvltrieeUpdate, FileMmap, Found, IdxFile};
use idx_file::{AvltrieeOrd, IdxFileAllocator};
pub use various_data_file::DataAddress;

use std::{
    cmp::Ordering,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::Path,
    thread,
};

use various_data_file::VariousDataFile;

type IdxBinaryAllocator = IdxFileAllocator<DataAddress>;

pub struct IdxBinary {
    index: IdxFile<DataAddress, [u8]>,
    data_file: VariousDataFile,
}

impl Deref for IdxBinary {
    type Target = IdxFile<DataAddress, [u8]>;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}
impl DerefMut for IdxBinary {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

impl AsRef<Avltriee<DataAddress, [u8], IdxBinaryAllocator>> for IdxBinary {
    fn as_ref(&self) -> &Avltriee<DataAddress, [u8], IdxBinaryAllocator> {
        self
    }
}
impl AsMut<Avltriee<DataAddress, [u8], IdxBinaryAllocator>> for IdxBinary {
    fn as_mut(&mut self) -> &mut Avltriee<DataAddress, [u8], IdxBinaryAllocator> {
        self
    }
}

impl AvltrieeOrd<DataAddress, [u8], IdxBinaryAllocator> for IdxBinary {
    fn cmp(&self, left: &DataAddress, right: &[u8]) -> Ordering {
        compare(self.data_file.bytes(left), right)
    }
}

impl AvltrieeUpdate<DataAddress, [u8], IdxFileAllocator<DataAddress>> for IdxBinary {
    fn convert_value(&mut self, input: &[u8]) -> DataAddress {
        self.data_file.insert(input).address().clone()
    }

    fn delete_before_update(&mut self, row: NonZeroU32) {
        let unique_value = if let Some((true, node)) = self.index.is_unique(row) {
            Some((**node).clone())
        } else {
            None
        };
        thread::scope(|s| {
            let h1 = s.spawn(|| {
                if let Some(unique_value) = unique_value {
                    self.data_file.delete(unique_value);
                }
            });
            let h2 = s.spawn(|| self.index.delete(row));

            h1.join().unwrap();
            h2.join().unwrap();
        });
    }
}

impl IdxBinary {
    /// Opens the file and creates the IdxBinary.
    /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let path = path.as_ref();
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

    /// Returns the value of the specified row. Returns None if the row does not exist.
    pub fn bytes(&self, row: NonZeroU32) -> Option<&[u8]> {
        self.index.get(row).map(|v| self.data_file.bytes(&v))
    }

    /// Updates the byte string of the specified row.
    /// If row does not exist, it will be expanded automatically..
    pub fn update(&mut self, row: NonZeroU32, content: &Vec<u8>) {
        Avltriee::update_with(self, row, content);
    }
}
