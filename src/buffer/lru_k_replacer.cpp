//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

void LRUKNode::SetEvictable(bool evictable) { this->is_evictable_ = evictable; }

auto LRUKNode::IsEvictable() -> bool { return this->is_evictable_; }

auto LRUKNode::GetFid() -> frame_id_t { return this->fid_; }

auto LRUKNode::GetBackwardKDist(size_t current_timestamp) -> size_t {
  if (this->HasInfBackwardKDist()) {
    return SIZE_T_MAX;
  }
  return current_timestamp - this->history_.front();
}

auto LRUKNode::HasInfBackwardKDist() -> bool { return this->history_.size() < this->k_; }

auto LRUKNode::GetEarliestTimestamp() -> size_t {
  if (this->history_.size() == 0) {
    return 0;
  }
  return this->history_.front();
}

void LRUKNode::InsertHistoryTimestamp(size_t current_timestamp) {
  this->history_.push_back(current_timestamp);
  if (this->history_.size() > this->k_) {
    this->history_.pop_front();
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    for(size_t f_index = 0; f_index < num_frames; f_index++) {
        this->node_store_.insert(std::pair(f_index, LRUKNode(k, f_index)));
    }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    this->node_store_.find(frame_id)->second.InsertHistoryTimestamp(timestamp);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
