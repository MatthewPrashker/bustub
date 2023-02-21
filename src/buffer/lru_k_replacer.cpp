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
    return INT64_MAX;
  }
  return current_timestamp - this->history_.front();
}

auto LRUKNode::HasInfBackwardKDist() -> bool { return this->history_.size() < this->k_; }

auto LRUKNode::GetEarliestTimestamp() -> size_t {
  if (this->history_.empty()) {
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

void LRUKNode::ClearHistory() { this->history_.clear(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  std::lock_guard<std::mutex> lk(this->latch_);
  for (size_t fid = 0; fid < num_frames; fid++) {
    this->node_store_.insert({fid, LRUKNode(k, fid)});
  }
}

LRUKReplacer::~LRUKReplacer() = default;

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  std::vector<frame_id_t> inf_back_ids;
  std::vector<frame_id_t> n_inf_back_ids;

  for (auto &node_it : this->node_store_) {
    auto node = node_it.second;
    if (!node.IsEvictable()) {
      continue;
    }
    if (node.HasInfBackwardKDist()) {
      inf_back_ids.push_back(node.GetFid());
    } else {
      n_inf_back_ids.push_back(node.GetFid());
    }
  }
  if (!inf_back_ids.empty()) {
    // There is at least one node with infinite backwards k-distance,
    // So among these nodes, we look at the one with the earliest time-stamp
    auto evict_id = inf_back_ids[0];
    auto earliest_ts = this->node_store_.find(inf_back_ids[0])->second.GetEarliestTimestamp();
    for (size_t i = 1; i < inf_back_ids.size(); i++) {
      auto cur_ts = this->node_store_.find(inf_back_ids[i])->second.GetEarliestTimestamp();
      if (cur_ts < earliest_ts) {
        earliest_ts = cur_ts;
        evict_id = inf_back_ids[i];
      }
    }
    *frame_id = evict_id;
    this->RemoveUnsync(evict_id);
    return true;
  }
  if (!n_inf_back_ids.empty()) {
    // Evict the node with the largest backwards k-dist
    auto timestamp = this->current_timestamp_;
    auto evict_id = n_inf_back_ids[0];
    auto largest_k_dist = this->node_store_.find(n_inf_back_ids[0])->second.GetBackwardKDist(timestamp);
    for (size_t i = 1; i < n_inf_back_ids.size(); i++) {
      auto cur_k_dist = this->node_store_.find(n_inf_back_ids[i])->second.GetBackwardKDist(timestamp);
      if (cur_k_dist > largest_k_dist) {
        largest_k_dist = cur_k_dist;
        evict_id = n_inf_back_ids[i];
      }
    }
    *frame_id = evict_id;
    this->RemoveUnsync(evict_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lk(this->latch_);
  auto timestamp = this->current_timestamp_++;

  BUSTUB_ASSERT(this->node_store_.find(frame_id) != this->node_store_.end(),
                "Trying to Record Access to Invalid Frame");
  this->node_store_.find(frame_id)->second.InsertHistoryTimestamp(timestamp);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(this->latch_);
  this->SetEvictableUnsync(frame_id, set_evictable);
}

void LRUKReplacer::SetEvictableUnsync(frame_id_t frame_id, bool set_evictable) {
  auto frame_it = this->node_store_.find(frame_id);
  BUSTUB_ASSERT(frame_it != this->node_store_.end(), "Evicting Frame which does not Exist");

  auto prev_evictable = frame_it->second.IsEvictable();
  frame_it->second.SetEvictable(set_evictable);

  if (prev_evictable) {
    --this->curr_size_;
  }
  if (set_evictable) {
    ++this->curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(this->latch_);
  this->RemoveUnsync(frame_id);
}

void LRUKReplacer::RemoveUnsync(frame_id_t frame_id) {
  auto frame_it = this->node_store_.find(frame_id);
  if (frame_it == this->node_store_.end() || !frame_it->second.IsEvictable()) {
    return;
  }

  frame_it->second.ClearHistory();
  this->SetEvictableUnsync(frame_id, false);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lk(this->latch_);
  return this->curr_size_;
}

}  // namespace bustub
