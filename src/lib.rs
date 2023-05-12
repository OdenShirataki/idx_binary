use std::{cmp::Ordering, io, path::Path};

pub use natord;

use anyhow::Result;
pub use idx_file::{anyhow, Avltriee, AvltrieeHolder, AvltrieeIter, FileMmap, Found, IdxFile};
use various_data_file::{DataAddress, VariousDataFile};

#[derive(Clone, PartialEq, Debug)]
pub struct Entity {
    data_address: DataAddress,
    num: f64,
}

impl Entity {
    pub fn new(data_address: &DataAddress, num: f64) -> Self {
        Self {
            data_address: data_address.clone(),
            num,
        }
    }
    pub fn data_address(&self) -> &DataAddress {
        &self.data_address
    }
    pub fn num(&self) -> f64 {
        self.num
    }
}

pub struct IdxBinary {
    index: IdxFile<Entity>,
    data_file: VariousDataFile,
}

impl AvltrieeHolder<Entity, &[u8]> for IdxBinary {
    fn triee(&self) -> &Avltriee<Entity> {
        self.triee()
    }
    fn triee_mut(&mut self) -> &mut Avltriee<Entity> {
        self.index.triee_mut()
    }
    fn cmp(&self, left: &Entity, right: &&[u8]) -> Ordering {
        self.cmp(left, right)
    }

    fn search(&self, input: &&[u8]) -> Found {
        self.index.triee().search_uord(|data| self.cmp(data, input))
    }

    fn value(&mut self, input: &[u8]) -> Result<Entity> {
        let data_address = self.data_file.insert(input)?;
        Ok(Entity::new(
            data_address.address(),
            unsafe { std::str::from_utf8_unchecked(input) }
                .parse()
                .unwrap_or(0.0),
        ))
    }

    fn delete(&mut self, row: u32, delete_node: &Entity) -> Result<()> {
        if !unsafe { self.index.triee().has_same(row) } {
            self.data_file.delete(&delete_node.data_address()).unwrap();
        }
        self.index.delete(row)?;
        Ok(())
    }
}

impl IdxBinary {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        Ok(Self {
            index: IdxFile::new({
                let mut path = path.to_path_buf();
                path.push(".i");
                path
            })?,
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.push(".d");
                path
            })?,
        })
    }

    pub fn triee(&self) -> &Avltriee<Entity> {
        self.index.triee()
    }

    pub fn get(&self, row: u32) -> Option<&'static [u8]> {
        if let Some(e) = self.index.value(row) {
            Some(unsafe { self.data_file.bytes(e.data_address()) })
        } else {
            None
        }
    }
    pub fn num(&self, row: u32) -> Option<f64> {
        if let Some(e) = self.index.value(row) {
            Some(e.num())
        } else {
            None
        }
    }
    pub fn update(&mut self, row: u32, content: &[u8]) -> Result<u32> {
        let row = self.index.new_row(row)?;
        unsafe {
            Avltriee::update_holder(self, row, content)?;
        }
        Ok(row)
    }
    pub fn delete(&mut self, row: u32) -> std::io::Result<()> {
        self.index.delete(row)?;
        Ok(())
    }

    pub fn cmp(&self, data: &Entity, content: &[u8]) -> Ordering {
        let left = unsafe { self.data_file.bytes(data.data_address()) };
        if left == content {
            Ordering::Equal
        } else {
            unsafe {
                natord::compare(
                    std::str::from_utf8_unchecked(left),
                    std::str::from_utf8_unchecked(content),
                )
            }
        }
    }
}
