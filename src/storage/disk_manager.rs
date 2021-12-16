use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::Path,
    sync::Mutex, io::{BufWriter, Seek, SeekFrom, Write, Read},
};

use crate::{error::Result, storage::PAGE_SIZE};

/// Page Device, if i have other implement about read/write page, i can implement it
/// e.g. network data read and write
pub trait PageDevice {
    fn write_page(&self, page_id: u32, page_data: &[u8]) -> Result<usize>;

    fn read_page(&self, page_id: u32, page_data: &mut [u8]) -> Result<usize>;
}

pub struct DiskManager {
    file: Mutex<File>,
}

impl DiskManager {
    pub fn new(dir: &Path) -> Result<DiskManager> {
        create_dir_all(dir)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("mydb.db"))?;
        Ok(DiskManager {
            file: Mutex::new(file),
        })
    }
}

impl PageDevice for DiskManager {
    
    fn write_page(&self, page_id: u32, page_data: &[u8]) -> Result<usize> {
        let mut write_len = page_data.len();
        if write_len > PAGE_SIZE {
            write_len = PAGE_SIZE;
        } else if write_len == 0 {
            return Ok(0)
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        let write_data = &page_data[..write_len];
        let mut file = self.file.lock()?;
        let mut writer = BufWriter::new(&mut *file);

        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(write_data)?;
        writer.flush()?;

        drop(writer);

        Ok(write_len)
    }

    fn read_page(&self, page_id: u32, page_data: &mut [u8]) -> Result<usize> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut read_len = page_data.len();
        if read_len > PAGE_SIZE {
            read_len = PAGE_SIZE;
        } else if read_len == 0 {
            return Ok(0);
        }
        let mut file = self.file.lock()?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        if offset >= file_size {
            return Ok(0);
        }
        if read_len > file_size - offset {
            read_len = file_size - offset;
        }

        let mut read_buf = &mut page_data[..read_len];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(read_buf)?;

        Ok(read_len)
    }

}

#[cfg(test)]
mod test {
    use crate::{error::Result, storage::PAGE_SIZE};

    use super::{PageDevice, DiskManager};

    struct Test {
        page_id: u32,
        data: Vec<u8>
    }

    #[test]
    fn test() -> Result<()> {
        let dir = tempdir::TempDir::new("mydb")?;
        let page_device: dyn PageDevice = DiskManager::new(dir)?;
        let tests = [
            Test {
                page_id: 0,
                data: Vec::from("Hello World!!".as_bytes())
            }
        ];

        for test in tests {
            let data = test.data;
            let page_id = test.page_id;

            let data_size = data.len();
            let write_size = page_device.write_page(page_id, &data)?;
            assert_eq!(data_size, write_size, "write size test fail!!");


            let mut read_buf = [0u8; PAGE_SIZE];
            let read_size = page_device.read_page(page_id, &mut read_buf)?;
            assert_eq!(data_size, read_size, "read size test fail!!");
        }
        Ok(())
    }

}