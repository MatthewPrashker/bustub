//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetFreeFrame(frame_id_t *frame_id) -> bool {
    if(this->free_list_.size() > 0) {
        *frame_id = this->free_list_.front();
        this->free_list_.pop_front();
        return true;
    } else {
        // Try to evict a page
        if(this->replacer_->Evict(frame_id)) {
            return true;
        }
    }
    return false;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
    frame_id_t n_frame;
    if(!this->GetFreeFrame(&n_frame)) {return nullptr;}

    // Handle existing page in the frame
    auto e_page = &this->pages_[n_frame];
    if(e_page->IsDirty()) {
        this->disk_manager_->WritePage(e_page->page_id_, e_page->data_);
    }

    // Reset page at the frame
    e_page->ResetMemory();
    page_id_t n_page_id = this->AllocatePage();
    e_page->page_id_ = n_page_id;
    this->replacer_->SetEvictable(n_frame, false);
    this->replacer_->RecordAccess(n_frame);

    this->page_table_[n_page_id] = n_frame;
    *page_id = n_page_id;
    return e_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * { return nullptr; }

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool { return false; }

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {

}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
