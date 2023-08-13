mod compare;

pub use compare::compare;
pub use idx_file::{Avltriee, AvltrieeHolder, AvltrieeIter, FileMmap, Found, IdxFile};
pub use various_data_file::DataAddress;

use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
    path::Path,
};

use various_data_file::VariousDataFile;

pub trait DataAddressHolder<T> {
    fn data_address(&self) -> &DataAddress;
    fn new(data_address: DataAddress, input: &[u8]) -> T;
}

pub struct IdxBinary<T> {
    index: IdxFile<T>,
    data_file: VariousDataFile,
}
impl<T> Deref for IdxBinary<T> {
    type Target = IdxFile<T>;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}
impl<T> DerefMut for IdxBinary<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

impl<T: DataAddressHolder<T>> AvltrieeHolder<T, &[u8]> for IdxBinary<T> {
    fn triee(&self) -> &Avltriee<T> {
        self
    }
    fn triee_mut(&mut self) -> &mut Avltriee<T> {
        self
    }
    fn cmp(&self, left: &T, right: &&[u8]) -> Ordering {
        self.cmp(left, right)
    }

    fn search_end(&self, input: &&[u8]) -> Found {
        self.index.search_end(|data| self.cmp(data, input))
    }

    fn value(&mut self, input: &[u8]) -> T {
        T::new(self.data_file.insert(input).address().clone(), input)
    }

    fn delete_before_update(&mut self, row: u32, delete_node: &T) {
        if !unsafe { self.index.has_same(row) } {
            self.data_file.delete(&delete_node.data_address());
        }
        self.index.delete(row);
        self.index.new_row(row);
    }
}

impl<T: DataAddressHolder<T>> IdxBinary<T> {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        Self {
            index: IdxFile::new({
                let mut path = path.to_path_buf();
                path.push(".i");
                path
            }),
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.push(".d");
                path
            }),
        }
    }
    pub fn bytes(&self, row: u32) -> Option<&'static [u8]> {
        if let Some(value) = self.index.value(row) {
            Some(unsafe { self.data_file.bytes(&value.data_address()) })
        } else {
            None
        }
    }

    pub fn update(&mut self, row: u32, content: &[u8]) -> u32
    where
        T: Clone,
    {
        let row = self.index.new_row(row);
        unsafe {
            Avltriee::update_holder(self, row, content);
        }
        row
    }
    pub fn cmp(&self, data: &T, content: &[u8]) -> Ordering {
        let left = unsafe { self.data_file.bytes(data.data_address()) };
        if left == content {
            Ordering::Equal
        } else {
            compare(left, content)
        }
    }
}
