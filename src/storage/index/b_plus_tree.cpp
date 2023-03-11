#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  std::cout << "Constructing tree with leaf_max_size " << leaf_max_size << " "
            << " internal max size " << internal_max_size << "\n";
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/

// TODO(mprashker): Replace with binary search
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetChildIndex(const InternalPage *page, const KeyType &key) const -> page_id_t {
  if (this->comparator_(key, page->KeyAt(1)) < 0) {
    return page->ValueAt(0);
  }
  for (int i = 1; i + 1 < page->GetSize(); i++) {
    auto l_key = page->KeyAt(i);
    auto r_key = page->KeyAt(i + 1);
    if (this->comparator_(l_key, key) <= 0 && this->comparator_(key, r_key) < 0) {
      return page->ValueAt(i);
    }
  }
  return page->ValueAt(page->GetSize() - 1);
}

// TODO(mprashker): Replace with binary search
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindValueInLeaf(const LeafPage *page, const KeyType &key, std::vector<ValueType> *result) const
    -> bool {
  bool found = false;
  for (int i = 0; i < page->GetSize(); i++) {
    if (this->comparator_(page->KeyAt(i), key) == 0) {
      found = true;
      result->push_back(page->ValueAt(i));
    }
  }
  return found;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (this->IsEmpty()) {
    return false;
  }
  ReadPageGuard cur_guard = this->bpm_->FetchPageRead(this->GetRootPageId());
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
    auto child_pid = this->GetChildIndex(internal_page, key);

    // Update cur to child
    cur_guard = this->bpm_->FetchPageRead(child_pid);
    cur_page = cur_guard.As<BPlusTreePage>();
  }

  auto leaf_page = reinterpret_cast<const LeafPage *>(cur_page);
  return this->FindValueInLeaf(leaf_page, key, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MakeNewRoot(bool as_leaf) -> page_id_t {
  page_id_t root_page_id;
  this->bpm_->NewPage(&root_page_id);
  this->bpm_->UnpinPage(root_page_id, false);

  // set new root id in the header page
  WritePageGuard header_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  BUSTUB_ASSERT(header_page != nullptr, "Failed to allocate page in Make New Root");

  header_page->root_page_id_ = root_page_id;

  // Initialize the root page
  WritePageGuard guard = this->bpm_->FetchPageWrite(root_page_id);

  if (as_leaf) {
    auto root = guard.AsMut<LeafPage>();
    root->Init(this->leaf_max_size_);
  } else {
    auto root = guard.AsMut<InternalPage>();
    root->Init(this->internal_max_size_);
  }
  return root_page_id;
}

// TODO(mprashker): Replace with binary search
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertEntryInLeaf(LeafPage *page, page_id_t page_id, const KeyType &key, const ValueType &val,
                                       Context *ctx) -> bool {
  int insert_index = 0;
  while (insert_index < page->GetSize() && this->comparator_(page->KeyAt(insert_index), key) < 0) {
    insert_index++;
  }
  if (insert_index < page->GetSize() && this->comparator_(page->KeyAt(insert_index), key) == 0) {
    // key already exists
    return false;
  }
  std::vector<MappingType> tmp;
  for (int i = insert_index; i < page->GetSize(); i++) {
    tmp.emplace_back(page->KeyAt(i), page->ValueAt(i));
  }
  page->SetKeyAndValueAt(insert_index, key, val);
  for (auto kv : tmp) {
    page->SetKeyAndValueAt(++insert_index, kv.first, kv.second);
  }
  page->IncreaseSize(1);

  if (this->LeafPageFull(page)) {
    this->SplitLeafNode(page, page_id, ctx);
  }
  return true;
}

// TODO(mprashker): Replace with binary search
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertEntryInInternal(InternalPage *page, page_id_t page_id, const KeyType &key,
                                           const page_id_t &value, Context *ctx, bool replace) -> bool {
  int insert_index = 1;
  while (insert_index < page->GetSize() && this->comparator_(page->KeyAt(insert_index), key) < 0) {
    insert_index++;
  }
  if (insert_index < page->GetSize() && this->comparator_(page->KeyAt(insert_index), key) == 0) {
    // key already exists
    if (replace) {
      page->SetValueAt(insert_index, value);
      return true;
    }
    return false;
  }
  std::vector<std::pair<KeyType, page_id_t>> tmp;
  for (int i = insert_index; i < page->GetSize(); i++) {
    tmp.emplace_back(page->KeyAt(i), page->ValueAt(i));
  }
  page->SetKeyAndValueAt(insert_index, key, value);
  for (auto kv : tmp) {
    page->SetKeyAndValueAt(++insert_index, kv.first, kv.second);
  }
  page->IncreaseSize(1);

  if (this->InternalPageFull(page)) {
    this->SplitInternalNode(page, page_id, ctx);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafPageFull(LeafPage *page) const -> bool { return (page->GetSize() == page->GetMaxSize()); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalPageFull(InternalPage *page) const -> bool {
  return (page->GetSize() > page->GetMaxSize());
}

// TODO (mprashker)
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalCanAbsorbInsert(const InternalPage *page) const -> bool { return false; }

// TODO (mprashker)
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalCanAbsorbDelete(const InternalPage *page) const -> bool { return false; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalNode(InternalPage *old_internal, page_id_t old_internal_id, Context *ctx)
    -> page_id_t {
  page_id_t new_internal_id;
  this->bpm_->NewPage(&new_internal_id);
  this->bpm_->UnpinPage(new_internal_id, false);

  WritePageGuard guard = this->bpm_->FetchPageWrite(new_internal_id);
  auto new_internal = guard.AsMut<InternalPage>();
  BUSTUB_ASSERT(new_internal != nullptr, "Failed to allocate page in SplitInternalNode");

  new_internal->Init(this->internal_max_size_);

  // Insert entries into new internal node
  auto min_size = old_internal->GetMinSize();
  new_internal->SetValueAt(0, old_internal->ValueAt(min_size));
  new_internal->IncreaseSize(1);
  for (int i = 1; i + min_size < old_internal->GetSize(); i++) {
    auto key = old_internal->KeyAt(i + min_size);
    auto val = old_internal->ValueAt(i + min_size);
    this->InsertEntryInInternal(new_internal, new_internal_id, key, val, ctx);
  }
  auto up_key = old_internal->KeyAt(min_size);
  auto up_val = old_internal->ValueAt(min_size);

  // Truncate the old internal node
  old_internal->SetSize(old_internal->GetMinSize());
  bool internal_is_root = (this->GetRootPageId() == old_internal_id);
  if (internal_is_root) {
    // Make root as internal node
    auto new_root_id = this->MakeNewRoot(false);
    WritePageGuard root = this->bpm_->FetchPageWrite(new_root_id);
    auto root_page = root.AsMut<InternalPage>();

    root_page->IncreaseSize(1);
    root_page->SetValueAt(0, old_internal_id);
    this->InsertEntryInInternal(root_page, new_root_id, up_key, new_internal_id, ctx);
  } else {
    auto parent_guard = std::move(ctx->write_set_.back().first);
    auto parent_id = ctx->write_set_.back().second;
    ctx->write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();

    new_internal->SetValueAt(0, up_val);
    this->InsertEntryInInternal(parent_page, parent_id, up_key, new_internal_id, ctx, true);
  }
  return new_internal_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage *old_leaf, page_id_t old_leaf_id, Context *ctx) -> page_id_t {
  page_id_t new_leaf_id;
  this->bpm_->NewPage(&new_leaf_id);
  this->bpm_->UnpinPage(new_leaf_id, false);

  WritePageGuard guard = this->bpm_->FetchPageWrite(new_leaf_id);
  auto new_leaf = guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(new_leaf != nullptr, "Failed to allocate page in SplitLeafNode");

  new_leaf->Init(this->leaf_max_size_);

  // Insert latter half of entries into new_leaf
  auto min_size = old_leaf->GetMinSize();
  for (int i = 0; i + min_size < old_leaf->GetSize(); i++) {
    auto key = old_leaf->KeyAt(i + min_size);
    auto val = old_leaf->ValueAt(i + min_size);
    this->InsertEntryInLeaf(new_leaf, new_leaf_id, key, val, ctx);
  }
  old_leaf->SetNextPageId(new_leaf_id);
  // Truncate existing entries from the node
  old_leaf->SetSize(old_leaf->GetMinSize());
  bool leaf_is_root = (this->GetRootPageId() == old_leaf_id);
  if (leaf_is_root) {
    // Make root as internal node
    auto new_root_id = this->MakeNewRoot(false);
    WritePageGuard root = this->bpm_->FetchPageWrite(new_root_id);
    auto root_page = root.AsMut<InternalPage>();

    root_page->SetValueAt(0, old_leaf_id);
    root_page->IncreaseSize(1);
    this->InsertEntryInInternal(root_page, this->GetRootPageId(), new_leaf->KeyAt(0), new_leaf_id, ctx);
  } else {
    auto parent_guard = std::move(ctx->write_set_.back().first);
    auto parent_id = ctx->write_set_.back().second;
    ctx->write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    this->InsertEntryInInternal(parent_page, parent_id, new_leaf->KeyAt(0), new_leaf_id, ctx, true);
  }
  return new_leaf_id;
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  std::cout << "Inserting " << key << "\n";
  if (this->IsEmpty()) {
    // Make new root as leaf node
    this->MakeNewRoot(true);
  }
  // Iterate from the root until we find a non-leaf node.
  Context ctx;
  WritePageGuard cur_guard = this->bpm_->FetchPageWrite(this->GetRootPageId());
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();

  page_id_t cur_pid = this->GetRootPageId();
  while (!cur_page->IsLeafPage()) {
    // We will never modify nodes above cur_page,
    // so it is safe to release all locks above this node
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
    if (this->InternalCanAbsorbInsert(internal_page)) {
      ctx.UnlockWriteSet();
    }

    ctx.write_set_.emplace_back(std::move(cur_guard), cur_pid);

    // Update cur to child
    cur_pid = this->GetChildIndex(internal_page, key);
    cur_guard = this->bpm_->FetchPageWrite(cur_pid);
    cur_page = cur_guard.AsMut<BPlusTreePage>();
  }

  auto *leaf_page = cur_guard.AsMut<LeafPage>();
  return this->InsertEntryInLeaf(leaf_page, cur_pid, key, value, &ctx);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

// TODO (mprashker)
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveEntryInInternal(InternalPage *page, page_id_t page_id, const KeyType &key, Context *ctx)
    -> bool {
  return false;
}

// TODO (mprashker)
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveEntryInLeaf(LeafPage *page, page_id_t page_id, const KeyType &key, Context *ctx) -> bool {
  return false;
}

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = this->bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
