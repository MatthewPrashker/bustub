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
  flush_threshold_ = pool_size_ / 3;
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every frame is in the free list.
  for (frame_id_t fid = 0; static_cast<size_t>(fid) < pool_size_; ++fid) {
    free_list_.emplace_back(fid);
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// Only call while holding latch
// Attempts to find any free-frame by first looking in the free list
// and then trying to find a frame from which we can evict
auto BufferPoolManager::GetFreeFrame(frame_id_t *frame_id) -> bool {
  if (!this->free_list_.empty()) {
    *frame_id = this->free_list_.front();
    this->free_list_.pop_front();
    return true;
  }
  // Try to evict a page
  return this->replacer_->Evict(frame_id);
}

// Only call while holding latch
void BufferPoolManager::ReplaceFrame(frame_id_t frame_id, page_id_t n_page_id) {
  auto e_page = &this->pages_[frame_id];
  if (e_page->IsDirty()) {
    this->disk_manager_->WritePage(e_page->page_id_, e_page->data_);
  }

  this->page_table_.erase(e_page->page_id_);

  e_page->ResetMemory();
  e_page->page_id_ = n_page_id;
  e_page->pin_count_ = 1;
  e_page->is_dirty_ = false;

  this->replacer_->ResetFrameHistory(frame_id);
  this->replacer_->RecordAccess(frame_id);
  this->replacer_->SetEvictable(frame_id, false);
  this->page_table_[n_page_id] = frame_id;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(this->latch_);
  frame_id_t n_frame;
  if (!this->GetFreeFrame(&n_frame)) {
    return nullptr;
  }

  frame_id_t n_page_id = this->AllocatePage();
  this->ReplaceFrame(n_frame, n_page_id);

  *page_id = n_page_id;
  return &this->pages_[n_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(this->latch_);
  auto frame_it = this->page_table_.find(page_id);
  if (frame_it != this->page_table_.end()) {
    // Page is currently in Buffer Pool
    this->pages_[frame_it->second].pin_count_++;
    if (this->pages_[frame_it->second].pin_count_ > 0) {
      this->replacer_->SetEvictable(frame_it->second, false);
    }
    return &this->pages_[frame_it->second];
  }

  // At this point, we need to bring in the page from disk
  // So we find a frame we can use ...

  frame_id_t n_frame_id;
  if (!this->GetFreeFrame(&n_frame_id)) {
    return nullptr;
  }

  // ... clear the old page in that frame, and read in the new page.

  this->ReplaceFrame(n_frame_id, page_id);
  this->disk_manager_->ReadPage(page_id, this->pages_[n_frame_id].data_);
  return &this->pages_[n_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  auto page_it = this->page_table_.find(page_id);
  if (page_it == this->page_table_.end()) {
    return false;
  }

  auto frame_index = page_it->second;
  auto page = &this->pages_[frame_index];
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  --page->pin_count_;

  if (page->pin_count_ == 0) {
    this->replacer_->SetEvictable(frame_index, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  auto page_it = this->page_table_.find(page_id);
  if (page_it == this->page_table_.end()) {
    return false;
  }
  this->pending_flush_pages_.push_back(page_id);
  if (this->pending_flush_pages_.size() > this->flush_threshold_) {
    this->FlushPageBatch();
  }
  return true;
}

void BufferPoolManager::FlushPageBatch() {
  for (page_id_t flush_page : this->pending_flush_pages_) {
    auto page_it = this->page_table_.find(flush_page);
    auto frame_id = page_it->second;
    auto page = &this->pages_[frame_id];
    this->disk_manager_->WritePage(flush_page, page->data_);
    page->is_dirty_ = false;
  }
  this->pending_flush_pages_.clear();
}

void BufferPoolManager::FlushAllPages() {
  for (auto &page_it : this->page_table_) {
    auto page_id = page_it.first;
    this->FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  auto page_it = this->page_table_.find(page_id);
  if (page_it == this->page_table_.end()) {
    return true;
  }

  auto frame_id = page_it->second;
  auto page = &this->pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }

  this->page_table_.erase(page_it);
  this->replacer_->Remove(frame_id);
  this->free_list_.push_back(frame_id);

  page->ResetMemory();
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return BasicPageGuard{this, this->FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = this->FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return ReadPageGuard{this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = this->FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return WritePageGuard{this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return BasicPageGuard{this, this->NewPage(page_id)};
}

}  // namespace bustub
