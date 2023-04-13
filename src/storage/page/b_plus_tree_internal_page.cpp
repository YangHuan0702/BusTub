//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LikeVal(const KeyType &key, const KeyComparator &comparator) -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair,auto k) { return comparator(pair.first,k) < 0; });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize()-1);
  }
  return comparator(target->first,key) == 0 ? target->second : std::prev(target)->second;
}

//INDEX_TEMPLATE_ARGUMENTS
//void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert() {
//
//}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,const ValueType &new_value) -> int {
    int idx = ValueIndex(old_value) + 1;
    assert(idx > 0);
    IncreaseSize(1);
    int curSize = GetSize();
    for (int i = curSize - 1; i > idx; i--) {
        array_[i].first = array_[i - 1].first;
        array_[i].second = array_[i - 1].second;
    }
    array_[idx].first = new_key;
    array_[idx].second = new_value;
    return curSize;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &val) -> int{
    for (int i = 0; i < GetSize(); i++) {
        if (ValueAt(i) == val) {
            return i;
        }
    }
    return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,const ValueType &new_value) {
    int index_zero = 0;
    array_[index_zero].second = old_value;
    array_[index_zero + 1].first = new_key;
    array_[index_zero + 1].second = new_value;
    SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,BufferPoolManager *buffer_pool_manager) {
    assert(recipient != nullptr);
    int start_split_indx = GetMinSize();
    int original_size = GetSize();
    SetSize(start_split_indx);

    int size = original_size - start_split_indx;
    auto *items = array_ + start_split_indx;
    std::copy(items, items + size, array_ + GetSize());

    for (int i = 0; i < size; i++) {
        auto page = buffer_pool_manager->FetchPage(ValueAt(i + GetSize()));
        auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    }
    IncreaseSize(size);
//    int total = GetMaxSize() + 1;
//    assert(GetSize() == total);
//    int copyIdx = total/2;
//    page_id_t recipPageId = recipient->GetPageId();
//    for (int i = copyIdx; i < total; i++) {
//        recipient->array_[i - copyIdx].first = array_[i].first;
//        recipient->array_[i - copyIdx].second = array_[i].second;
//        auto childRawPage = buffer_pool_manager->FetchPage(array_[i].second);
//        auto *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
//        childTreePage->SetParentPageId(recipPageId);
//        buffer_pool_manager->UnpinPage(array_[i].second,true);
//    }
//    SetSize(copyIdx);
//    recipient->SetSize(total - copyIdx);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,const KeyComparator &comparator) const -> ValueType {
    assert(GetSize() > 1);
    int st = 1, ed = GetSize() - 1;
    while (st <= ed) { //find the last key in array <= input
        int mid = (ed - st) / 2 + st;
        if (comparator(array_[mid].first,key) <= 0) {
            st = mid + 1;
        } else {
            ed = mid - 1;
        }
    }
    return array_[st - 1].second;
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
