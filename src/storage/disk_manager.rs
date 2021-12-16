use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use crate::{error::Result, storage::PAGE_SIZE};

/// Page Device, if i have other implement about read/write page, i can implement it
/// e.g. network data read and write
pub trait PageDevice {
    fn write_page(&self, page_id: u32, page_data: &[u8; PAGE_SIZE]) -> Result<usize>;

    fn read_page(&self, page_id: u32, page_data: &mut [u8; PAGE_SIZE]) -> Result<usize>;
}

pub struct DiskManager {
    file: Mutex<File>,
}

impl DiskManager {
    pub fn open(dir: &Path) -> Result<DiskManager> {
        create_dir_all(dir)?;
        let file =
            OpenOptions::new().read(true).write(true).create(true).open(dir.join("mydb.db"))?;
        Ok(DiskManager { file: Mutex::new(file) })
    }
}

impl PageDevice for DiskManager {
    fn write_page(&self, page_id: u32, page_data: &[u8; PAGE_SIZE]) -> Result<usize> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut file = self.file.lock()?;
        let metadata = file.metadata()?;
        if offset > metadata.len() {
            return Ok(0);
        }

        let mut writer = BufWriter::new(&mut *file);
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(page_data)?;
        writer.flush()?;

        drop(writer);

        Ok(PAGE_SIZE)
    }

    fn read_page(&self, page_id: u32, page_data: &mut [u8; PAGE_SIZE]) -> Result<usize> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut file = self.file.lock()?;
        let metadata = file.metadata()?;
        if offset + PAGE_SIZE as u64 > metadata.len() {
            return Ok(0);
        }
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(page_data)?;

        Ok(PAGE_SIZE)
    }
}

#[cfg(test)]
mod test {
    use super::{DiskManager, PageDevice};
    use crate::{error::Result, storage::PAGE_SIZE};
    use tempfile::tempdir;

    struct Test {
        page_id: u32,
        data: Vec<u8>,
    }

    #[test]
    fn test() -> Result<()> {
        let dir = tempdir::TempDir::new("mydb")?;
        let page_device: Box<dyn PageDevice> = Box::new(DiskManager::open(dir.as_ref())?);
        let tests = [Test { page_id: 0, data: Vec::from("Hello World!!".as_bytes()) }];

        for test in tests {
            let data = test.data;
            let data_len = data.len();
            let page_id = test.page_id;
            let mut buf = [0u8; PAGE_SIZE];
            let mut write_buf = &mut buf[..data_len];
            write_buf.copy_from_slice(&data);
            let write_size = page_device.write_page(page_id, &buf)?;
            assert_eq!(PAGE_SIZE, write_size, "write size test fail!!");

            buf.fill(0u8);
            let read_size = page_device.read_page(page_id, &mut buf)?;
            assert_eq!(PAGE_SIZE, read_size, "read size test fail!!");

            assert_eq!(&data, &buf[..data_len], "disk value test fail!!");
        }
        Ok(())
    }
}
