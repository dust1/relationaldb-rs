use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use crate::storage::clock_replacer::ClockReplacer;
use crate::storage::disk_manager::DiskManager;
use crate::storage::PAGE_SIZE;

struct BufferPoolCache {
    cache: [u8; PAGE_SIZE],
    disk_manager: Arc<dyn DiskManager>,
    page_id: usize,
    modified: bool
}

pub struct BufferPoolManager {
    queue: VecDeque<(usize, Arc<Mutex<BufferPoolCache>>)>,
    disk_manager: Arc<dyn DiskManager>,
    pool_size: usize,
    /// [summary](https://github.com/cmu-db/bustub/blob/6f4e74f6eb5f56a13bac9b9b9bbc3b2a80b41258/src/include/buffer/buffer_pool_manager_instance.h#L127)
    num_instance: usize,
    instance_index: usize,
    next_page_id: AtomicUsize,
    clock_replacer: ClockReplacer
}

pub trait PoolManager {
    fn fetch_page(&mut self, page_id: usize) -> Option<Arc<Mutex<BufferPoolCache>>>;
    fn un_pin(&mut self, page_id: usize);
    fn flush_page(&mut self, page_id: usize);
    fn new_page(&mut self) -> Arc<Mutex<BufferPoolCache>>;
    fn delete_page(&mut self, page_id: usize);
    fn flush_all_page(&mut self);
}

/// 缓冲的可以参考操作系统教学中的block cache实现
/// 各个函数的定义：
/// https://github.com/cmu-db/bustub/blob/master/src/buffer/buffer_pool_manager_instance.cpp
/// https://github.com/cmu-db/bustub/blob/master/src/include/buffer/buffer_pool_manager_instance.h
impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<dyn DiskManager>) -> Self {
        Self {
            queue: VecDeque::new(),
            pool_size,
            disk_manager,
            num_instance : 1,
            instance_index: 0,
            next_page_id: AtomicUsize::new(0),
            clock_replacer: ClockReplacer::new(pool_size)
        }
    }

    fn allocate_page(&mut self) -> usize {
        let next_page_id = *self.next_page_id.get_mut();
        *self.next_page_id.get_mut() = next_page_id + self.num_instance;
        self.validate_page_id(next_page_id);
        next_page_id
    }

    fn deallocate_page(&mut self, _page_id: usize) {
        todo!()
    }

    fn validate_page_id(&self, page_id: usize) {
        assert_eq!(page_id % self.num_instance, self.instance_index)
    }

    fn check_queue(&mut self) {
        if self.queue.len() == self.pool_size {
            if let Some(remove_id) = self.clock_replacer.victim() {
                if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, pair)| pair.0 == remove_id) {
                    let (_, cache) = self.queue.remove(idx).unwrap();
                    cache.lock().unwrap().sync();
                } else {
                    panic!("Data is out of sync");
                }
            } else {
                if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, pair)| Arc::strong_count(&pair.1) == 1) {
                    let (_, cache) = self.queue.remove(idx).unwrap();
                    cache.lock().unwrap().sync();
                } else {
                    panic!("Run out of Cache");
                }
            }
        }
    }
}

impl PoolManager for BufferPoolManager {

    fn fetch_page(&mut self, page_id: usize) -> Option<Arc<Mutex<BufferPoolCache>>> {
        self.clock_replacer.pin(page_id);

        if let Some((_, cache)) = self.queue.iter().find(|(id, _)| page_id.eq(id)) {
            let page_cache = Arc::clone(&cache);
            return Some(page_cache);
        }
        if let Some(cache) = BufferPoolCache::read(page_id, Arc::clone(&self.disk_manager)) {
            self.check_queue();
            let page_cache = Arc::new(Mutex::new(cache));
            self.queue.push_back((page_id, Arc::clone(&page_cache)));
            return Some(page_cache);
        }

        None
    }

    fn un_pin(&mut self, page_id: usize) {
        self.clock_replacer.un_pin(page_id);
    }

    fn flush_page(&mut self, page_id: usize) {
        if let Some((_, cache)) = self.queue.iter().find(|(pid, _)| page_id.eq(pid)) {
            cache.lock().unwrap().sync();
        }
    }

    fn new_page(&mut self) -> Arc<Mutex<BufferPoolCache>> {
        let page_id = self.allocate_page();
        self.check_queue();

        let disk_manager = Arc::clone(&self.disk_manager);
        let cache = BufferPoolCache::create(page_id, disk_manager);
        let page_cache = Arc::new(Mutex::new(cache));
        self.queue.push_back((page_id, Arc::clone(&page_cache)));
        page_cache
    }

    fn delete_page(&mut self, page_id: usize) {
        if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, pair)| pair.0 == page_id) {
            let (_, cache) = self.queue.remove(idx).unwrap();
            self.clock_replacer.un_pin(page_id);
            cache.lock().unwrap().sync();
        }
    }

    fn flush_all_page(&mut self) {
        for (_, cache) in &self.queue {
            cache.lock().unwrap().sync();
        }
    }

}

impl BufferPoolCache {

    pub fn create(page_id: usize, disk_manager: Arc<dyn DiskManager>) -> Self {
        let page_data = [0u8; PAGE_SIZE];
        disk_manager.write_page(page_id, &page_data);
        Self {
            cache: page_data,
            disk_manager,
            page_id,
            modified: false
        }
    }

    pub fn read(page_id: usize, disk_manager: Arc<dyn DiskManager>) -> Option<Self> {
        let mut page_data = [0u8; PAGE_SIZE];
        if let Ok(state) = disk_manager.read_page(page_id, &mut page_data) {
            return match state {
                0 => {
                    None
                },
                _ => {
                    Some(Self {
                        cache: page_data,
                        disk_manager,
                        page_id,
                        modified: false
                    })
                }
            }
        }
        None
    }

    pub fn sync(&self) {
        if self.modified {
            self.disk_manager.write_page(self.page_id, &self.cache);
        }
    }

}

impl Drop for BufferPoolCache {
    fn drop(&mut self) {
        self.sync()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use crate::error::Result;
    use crate::storage::buffer_pool_manager::{BufferPoolManager, PoolManager};
    use crate::storage::disk_manager::{DiskManager, PageDevice};

    #[test]
    fn test() -> Result<()> {
        let dir = tempdir::TempDir::new("mydb")?;
        let disk_manager: Arc<dyn DiskManager> = Arc::new(PageDevice::open(dir.as_ref())?);
        let mut buffer_pool_manager:Box<dyn PoolManager> = Box::new(BufferPoolManager::new(4, disk_manager));

        let _header_page = buffer_pool_manager.new_page();
        {
            let table_page_1 = buffer_pool_manager.new_page();
            let page_id = table_page_1.lock().unwrap().page_id;
            buffer_pool_manager.un_pin(page_id);
        }
        let _table_page_2 = buffer_pool_manager.new_page();
        let _table_page_3 = buffer_pool_manager.new_page();
        {
            let _table_page_4 = buffer_pool_manager.new_page();
        }
        let mut table_page_5 = buffer_pool_manager.new_page();

        if let Some(_) = buffer_pool_manager.fetch_page(11) {
            assert!(false);
        }
        buffer_pool_manager.flush_page(5);
        Ok(())
    }

}