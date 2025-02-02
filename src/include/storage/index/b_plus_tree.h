/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<std::pair<WritePageGuard, page_id_t>> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }

  void UnlockWriteSet() {
    // Release locks in same order they were acquired,
    // so unlock from front to back
    while (!write_set_.empty()) {
      auto guard = std::move(write_set_.front().first);
      write_set_.pop_front();
    }
  }

  void UnlockReadSet() {
    while (!read_set_.empty()) {
      auto guard = std::move(read_set_.front());
      read_set_.pop_front();
    }
  }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  auto MakeNewRoot(bool as_leaf, Context *ctx) -> WritePageGuard;

  void SetRoot(page_id_t new_root_id, Context *ctx);

  auto GetSmallestKeyInSubTree(const BPlusTreePage *page) const -> KeyType;

  auto GetLargestKeyInSubTree(const BPlusTreePage *page) const -> KeyType;

  auto GetRootGuardSafe(Context *ctx, bool should_create = true) -> WritePageGuard;

  auto GetInternalIndexForKey(const InternalPage *page, const KeyType &key) const -> int;

  auto GetInternalIndexForValue(const InternalPage *page, page_id_t value) const -> int;

  auto GetChildPage(const InternalPage *page, const KeyType &key) const -> page_id_t;

  auto LeafContainingKey(const KeyType &key) const -> std::pair<bool, ReadPageGuard>;

  auto LeafPageFull(LeafPage *page) const -> bool;

  auto LeafPageTooSmall(LeafPage *page) const -> bool;

  auto InternalPageFull(InternalPage *page) const -> bool;

  auto InternalPageTooSmall(InternalPage *page) const -> bool;

  auto InternalCanAbsorbInsert(const InternalPage *page) const -> bool;

  auto LeafCanAbsorbInsert(const LeafPage *page) const -> bool;

  auto InternalCanAbsorbDelete(const InternalPage *page) const -> bool;

  auto LeafCanAbsorbDelete(const LeafPage *page) const -> bool;

  auto SplitLeafNode(LeafPage *old_leaf, page_id_t old_leaf_id, Context *ctx) -> page_id_t;

  auto SplitInternalNode(InternalPage *old_internal, page_id_t old_internal_id, Context *ctx) -> page_id_t;

  void CoalescesNode(BPlusTreePage *page, page_id_t page_id, const KeyType &key, Context *ctx);

  void MergeNodes(BPlusTreePage *left_page, page_id_t left_page_id, BPlusTreePage *right_page, page_id_t right_page_id,
                  InternalPage *parent_page, page_id_t parent_page_id, Context *ctx);

  void LeftShift(BPlusTreePage *left_page, page_id_t left_pid, BPlusTreePage *right_page, page_id_t right_pid,
                 InternalPage *parent_page);

  void RightShift(BPlusTreePage *left_page, page_id_t left_pid, BPlusTreePage *right_page, page_id_t right_pid,
                  InternalPage *parent_page);

  auto InsertEntryInLeaf(LeafPage *page, page_id_t page_id, const KeyType &key, const ValueType &val, Context *ctx)
      -> bool;

  auto AppendEntriesInLeaf(LeafPage *page, page_id_t page_id, const std::vector<std::pair<KeyType, ValueType>> &kvs,
                           Context *ctx);

  auto RemoveEntryInLeaf(LeafPage *page, page_id_t page_id, const KeyType &key, Context *ctx) -> bool;

  auto InsertEntryInInternal(InternalPage *page, page_id_t page_id, const KeyType &key, const page_id_t &value,
                             Context *ctx, bool replace = false) -> bool;

  auto AppendEntriesInInternal(InternalPage *page, page_id_t page_id,
                               const std::vector<std::pair<KeyType, page_id_t>> &kvs, Context *ctx);

  auto RemoveEntryInInternal(InternalPage *page, page_id_t page_id, const KeyType &key, Context *ctx) -> bool;

  auto FindValueInLeaf(const LeafPage *page, const KeyType &key, std::vector<ValueType> *result) const -> bool;

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  auto InsertOptimistic(const KeyType &key, const ValueType &value, Transaction *txn = nullptr)
      -> std::pair<bool, bool>;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  auto RemoveOptimistic(const KeyType &key, Transaction *txn) -> bool;

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() const -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printalbe B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
