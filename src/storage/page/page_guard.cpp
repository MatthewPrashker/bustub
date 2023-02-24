#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (this->bpm_ == nullptr) {
    return;
  }

  this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  this->bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that || that.bpm_ == nullptr) {
    return *this;
  }

  if (this->page_ == that.page_) {
    this->is_dirty_ = that.is_dirty_;
    return *this;
  }

  this->Drop();
  this->bpm_ = that.bpm_;
  this->page_ = this->bpm_->FetchPage(that.PageId());
  this->is_dirty_ = that.is_dirty_;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = BasicPageGuard(std::move(that.guard_)); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  this->guard_.Drop();
  this->guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  this->guard_ = BasicPageGuard(std::move(that.guard_));
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  this->guard_.Drop();
  this->guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() { this->Drop(); }
}  // namespace bustub
