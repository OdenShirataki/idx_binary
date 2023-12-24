mod compare;

pub use compare::compare;
pub use idx_file::{Avltriee, AvltrieeHolder, AvltrieeIter, FileMmap, Found, IdxFile};
pub use various_data_file::DataAddress;

use std::{
    cmp::Ordering,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::Path,
};

use async_trait::async_trait;
use various_data_file::VariousDataFile;

pub trait DataAddressHolder<T> {
    fn data_address(&self) -> &DataAddress;
    fn new(data_address: DataAddress, input: &[u8]) -> T;
}

impl DataAddressHolder<DataAddress> for DataAddress {
    fn data_address(&self) -> &DataAddress {
        &self
    }

    fn new(data_address: DataAddress, _: &[u8]) -> DataAddress {
        data_address
    }
}

pub struct IdxBinary<T> {
    index: IdxFile<T>,
    data_file: VariousDataFile,
}

impl<T> AsRef<Avltriee<T>> for IdxBinary<T> {
    fn as_ref(&self) -> &Avltriee<T> {
        self
    }
}
impl<T> AsMut<Avltriee<T>> for IdxBinary<T> {
    fn as_mut(&mut self) -> &mut Avltriee<T> {
        self
    }
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

#[async_trait(?Send)]
impl<T: DataAddressHolder<T>> AvltrieeHolder<T, &[u8]> for IdxBinary<T> {
    fn cmp(&self, left: &T, right: &&[u8]) -> Ordering {
        self.cmp(left, right)
    }

    fn search_end(&self, input: &&[u8]) -> Found {
        self.index.search_end(|data| self.cmp(data, input))
    }

    fn value(&mut self, input: &[u8]) -> T {
        T::new(self.data_file.insert(input).address().clone(), input)
    }

    async fn delete_before_update(&mut self, row: NonZeroU32, delete_node: &T) {
        let is_unique = unsafe { self.index.is_unique(row) };
        futures::join!(
            async {
                if is_unique {
                    self.data_file.delete(delete_node.data_address());
                }
            },
            async {
                self.index.delete(row);
            }
        );
        self.index.allocate(row);
    }
}

impl<T: DataAddressHolder<T>> IdxBinary<T> {
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

    pub fn bytes(&self, row: NonZeroU32) -> Option<&'static [u8]> {
        self.index
            .value(row)
            .map(|value| unsafe { self.data_file.bytes(&value.data_address()) })
    }

    pub async fn update(&mut self, row: NonZeroU32, content: &[u8])
    where
        T: Copy,
    {
        self.index.allocate(row);
        unsafe {
            Avltriee::update_holder(self, row, content).await;
        }
    }

    pub fn cmp(&self, data: &T, content: &[u8]) -> Ordering {
        compare(
            unsafe { self.data_file.bytes(data.data_address()) },
            content,
        )
    }
}
