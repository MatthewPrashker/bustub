/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : page_(nullptr), page_id_(INVALID_PAGE_ID), index_in_page_(0) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return this->page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto data = this->page_->GetDataRef();
  return *(data + this->index_in_page_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (this->IsEnd()) {
    return *this;
  }
  this->index_in_page_++;
  if (this->index_in_page_ < this->page_->GetSize()) {
    return *this;
  }
  page_id_t next_page_index = this->page_->GetNextPageId();
  if (next_page_index != INVALID_PAGE_ID) {
    BasicPageGuard next_guard = this->bpm_->FetchPageBasic(next_page_index);
    auto next_page = next_guard.As<LeafPage>();
    this->page_ = next_page;
    this->page_id_ = next_page_index;
    this->index_in_page_ = 0;
  } else {
    this->page_ = nullptr;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
