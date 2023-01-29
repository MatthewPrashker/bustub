#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  size_t key_index = 0;
  auto cur = this->root_;
  if (cur == nullptr) {
    return nullptr;
  }
  while (key_index < key.size()) {
    char char_at = key.at(key_index);
    if (cur->children_.find(char_at) != cur->children_.end()) {
      cur = cur->children_.find(char_at)->second;
    } else {
      return nullptr;
    }
    key_index++;
  }

  auto target = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (target == nullptr) {
    return nullptr;
  }
  return target->value_.get();
}

template <class T>
auto Trie::PutFromNode(std::unique_ptr<TrieNode> new_node, std::string_view &key, size_t key_index,
                       std::shared_ptr<T> value) const -> std::unique_ptr<TrieNode> {
  if (key_index == key.size()) {
    new_node = std::make_unique<TrieNodeWithValue<T>>(new_node->children_, std::move(value));
    new_node->is_value_node_ = true;
    return new_node;
  }

  char char_at = key.at(key_index);
  std::unique_ptr<TrieNode> new_child;

  if (new_node->children_.find(char_at) != new_node->children_.end()) {
    auto child = new_node->children_.find(char_at)->second;
    new_child = child->Clone();
  } else {
    new_child = std::make_unique<TrieNode>();
  }

  new_child = PutFromNode(std::move(new_child), key, key_index + 1, value);

  if (new_node->children_.find(char_at) != new_node->children_.end()) {
    new_node->children_.find(char_at)->second = std::move(new_child);
  } else {
    new_node->children_.insert(std::pair(char_at, std::move(new_child)));
  }
  return new_node;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::unique_ptr<TrieNode> new_root;
  if (this->root_ != nullptr) {
    new_root = this->root_->Clone();
  } else {
    new_root = std::make_unique<TrieNode>();
  }
  auto val = std::make_shared<T>(std::move(value));
  new_root = PutFromNode(std::move(new_root), key, 0, val);

  return Trie(std::move(new_root));
}

auto Trie::RemoveFromNode(std::unique_ptr<TrieNode> new_node, std::string_view &key, size_t key_index) const
    -> std::unique_ptr<TrieNode> {
  if (key_index == key.size()) {
    new_node = std::make_unique<TrieNode>(new_node->children_);
    return new_node;
  }
  auto child = new_node->children_.find(key.at(key_index));
  if (child == new_node->children_.end()) {
    return new_node;
  }
  auto new_child = child->second->Clone();
  new_child = RemoveFromNode(std::move(new_child), key, key_index + 1);
  child->second = std::move(new_child);
  return new_node;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::unique_ptr<TrieNode> new_root;
  if (this->root_ != nullptr) {
    new_root = this->root_->Clone();
  } else {
    new_root = std::make_unique<TrieNode>();
  }
  new_root = RemoveFromNode(std::move(new_root), key, 0);
  return Trie(std::move(new_root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
