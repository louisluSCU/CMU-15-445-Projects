//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  
  pg_latch.lock();
  pt_latch.lock();

  if(page_table_.find(page_id) != page_table_.end()) {
    frame_id_t target = page_table_[page_id];
    
    replacer_->Pin(target);
    pages_[target].pin_count_ += 1;
    
    pg_latch.unlock();
    pt_latch.unlock();

    // LOG_INFO("Fetch page %d from bf", page_id);
    return &pages_[target];
  }

  else {
    frame_id_t target;
    fl_latch.lock();

    // search free list
    if(free_list_.size() != 0) {
      target = free_list_.front();
      free_list_.pop_front();
      
      fl_latch.unlock();

      replacer_->Pin(target);
      pages_[target].pin_count_ += 1;

      // read to buffer
      pages_[target].page_id_ = page_id;
      pages_[target].is_dirty_ = false;
      page_table_[page_id] = target;
      disk_manager_->ReadPage(page_id, pages_[target].data_);

      pg_latch.unlock();
      pt_latch.unlock();

      // LOG_INFO("Fetch page %d from fl", page_id);
      return &pages_[target];
    }

    // search from replacer
    else {
      fl_latch.unlock();

      // evict
      bool evi_suc = replacer_->Victim(&target);
      if(!evi_suc) return nullptr;
      page_id_t evict_page = pages_[target].GetPageId();

      if(pages_[target].IsDirty()) {
        bool flu_suc = FlushPageImpl(evict_page);
        if(!flu_suc) return nullptr;
      }

      replacer_->Pin(target);
      pages_[target].pin_count_ += 1;
      // read to buffer
      page_table_.erase(evict_page);
      pages_[target].page_id_ = page_id;
      pages_[target].is_dirty_ = false;
      page_table_[page_id] = target;
      disk_manager_->ReadPage(page_id, pages_[target].data_);

      pg_latch.unlock();
      pt_latch.unlock();

      // LOG_INFO("Fetch page %d from replacer", page_id);
      return &pages_[target];
    }
  }
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  pt_latch.lock();
  
  if(page_table_.find(page_id) == page_table_.end()) {
    pt_latch.unlock();
    // LOG_INFO("Unpin page %d from non-ex", page_id);
    return true;
  }
  else {
    pg_latch.lock();
    frame_id_t frame = page_table_[page_id];
    pt_latch.unlock();

    if(pages_[frame].GetPinCount() <= 0) {
      pg_latch.unlock();
      LOG_ERROR("Unpin page %d failed, pincnt <= 0", page_id);
      return false;
    }
    else {
      pages_[frame].pin_count_--;
      pages_[frame].is_dirty_ |= is_dirty;
      
      // move to replacer
      if(pages_[frame].GetPinCount() == 0) {
        replacer_->Unpin(frame);
      }

      pg_latch.unlock();
      // LOG_INFO("Unpin page %d from bf, present pin_cnt: %d", page_id, pages_[frame].pin_count_);
      return true;
    }
  }
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(page_table_.find(page_id) == page_table_.end()) {
    LOG_ERROR("Flush page %d failed, not found in bf", page_id);
    return false;
  }
  else {
    frame_id_t frame = page_table_[page_id];

    if(!pages_[frame].IsDirty()) {
      // LOG_INFO("Flush page %d not dirty", page_id);
      return true;
    }
    else {
      disk_manager_->WritePage(page_id, pages_[frame].data_);
      pages_[frame].is_dirty_ = false;

      // LOG_INFO("Flush page %d dirty", page_id);
      return true;
    }
  }
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  
  fl_latch.lock();
  pg_latch.lock();
  pt_latch.lock();

  // have free page
  if(free_list_.size() != 0) {
    frame_id_t free = free_list_.front();
    free_list_.pop_front();
    fl_latch.unlock();

    // read to buffer
    *page_id = disk_manager_->AllocatePage();
    pages_[free].ResetMemory();
    pages_[free].page_id_ = *page_id;
    pages_[free].pin_count_ = 1;
    pages_[free].is_dirty_ = false;
    replacer_->Pin(free);
    page_table_[*page_id] = free;
    
    pg_latch.unlock();
    pt_latch.unlock();

    // LOG_INFO("New page created from fl, %d", *page_id);
    return &pages_[free];
  }

  // no free page
  else {
    fl_latch.unlock();

    frame_id_t candi;
    bool evict_suc = replacer_->Victim(&candi);
    if(!evict_suc) {
      pg_latch.unlock();
      pt_latch.unlock();
      return nullptr;
    }
    page_id_t evict_page = pages_[candi].GetPageId();
    
    // flush dirty page
    if(pages_[candi].IsDirty()) {
      bool flu_suc = FlushPageImpl(evict_page);
      if(!flu_suc) {
        pg_latch.unlock();
        pt_latch.unlock();
        return nullptr;
      }
    }
    
    // read to buffer
    page_table_.erase(evict_page);
    *page_id = disk_manager_->AllocatePage();
    pages_[candi].ResetMemory();
    pages_[candi].page_id_ = *page_id;
    pages_[candi].pin_count_ = 1;
    pages_[candi].is_dirty_ = false;
    replacer_->Pin(candi);
    page_table_[*page_id] = candi;
    
    pg_latch.unlock();
    pt_latch.unlock();

    // LOG_INFO("New page created from replacer, %d", *page_id);
    return &pages_[candi];
  }
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  pt_latch.lock();

  // P not exist
  if(page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    pt_latch.unlock();
    // LOG_INFO("Del page %d suc, non-ex", page_id);
    return true;
  }

  frame_id_t target = page_table_[page_id];
  pg_latch.lock();

  // page in use
  if(pages_[target].GetPinCount() != 0) {
    pt_latch.unlock();
    pg_latch.unlock();
    LOG_ERROR("Del page %d failed, in use", page_id);
    return false;
  }
  else {
    disk_manager_->DeallocatePage(page_id);
    page_table_.erase(page_id);
    pages_[target].page_id_ = INVALID_PAGE_ID;
    pages_[target].is_dirty_ = false;
    
    fl_latch.lock();
    free_list_.push_back(target);

    pt_latch.unlock();
    pg_latch.unlock();
    fl_latch.unlock();

    // LOG_INFO("Del page %d suc, from bf", page_id);
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  pg_latch.lock();
  for(size_t i=0; i<pool_size_; i++) {
    if(pages_[i].IsDirty()) {
      FlushPageImpl(pages_[i].GetPageId());
    }
  }
  pg_latch.unlock();
  // LOG_INFO("All pages flushed");
}

}  // namespace bustub
