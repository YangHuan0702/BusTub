//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "include/storage/page/b_plus_tree_internal_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}



INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,const KeyComparator &comparator) const -> int {
    assert(GetSize() >= 0);
    int st = 0, ed = GetSize() - 1;
    while (st <= ed) {
        int mid = (ed - st) / 2 + st;
        if (comparator(array_[mid].first,key) >= 0){
            ed = mid - 1;
        }else {
            st = mid + 1;
        }
    }
    return ed + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key,const KeyComparator &comparator,ValueType *val_out) const -> bool {
  int key_index = KeyIndex(key,comparator);
  if(key_index == -1){
    return false;
  }
  *val_out = ValueAt(key_index);
  return true;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,const ValueType &val,const KeyComparator &comparator) {
    auto target = std::lower_bound(array_,array_ + GetSize(),key,[&comparator](const auto &pair,auto k) { return comparator(pair.first,k) < 0; });
    int target_index = GetSize();
    if (target != array_ + GetSize()) {
        for (int index = GetSize() - 1; ; index--) {
            array_[index+1] = array_[index];
            if(comparator(array_[index].first,target->first) == 0){
                target_index = index;
                break;
            }
        }
    }
    array_[target_index] = std::make_pair(key,val);
    IncreaseSize(1);
//    int idx = KeyIndex(key,comparator); //first larger than key
//    assert(idx >= 0);
//    IncreaseSize(1);
//    int curSize = GetSize();
//    for (int i = curSize - 1; i > idx; i--) {
//        array_[i].first = array_[i - 1].first;
//        array_[i].second = array_[i - 1].second;
//    }
//    array_[idx].first = key;
//    array_[idx].second = val;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DirectInsert(const KeyType &key, const ValueType &val) {
    array_[GetSize()] = std::make_pair(key,val);
    IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,const KeyComparator &comparator) const -> bool {
    int idx = KeyIndex(key,comparator);
    if (idx < GetSize() && comparator(array_[idx].first, key) == 0) {
        value = array_[idx].second;
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,__attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    assert(recipient != nullptr);
    int total = GetMaxSize() + 1;
    assert(GetSize() == total);
    int copyIdx = total/2;
    for (int i = copyIdx; i < total; i++) {
        recipient->array_[i - copyIdx].first = array_[i].first;
        recipient->array_[i - copyIdx].second = array_[i].second;
    }
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
    SetSize(copyIdx);
    recipient->SetSize(total - copyIdx);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
    assert(index >= 0 && index < GetSize());
    return array_[index];
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndexPrecise(const KeyType &key,const KeyComparator &comparator) const -> int {
    for(int index = 0; index < GetSize(); index++){
        if(comparator(array_[index].first,key) == 0){
            return index;
        }
    }
    return -1;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key,const KeyComparator &comparator) {
    int index = KeyIndexPrecise(key,comparator);
    if (index == -1) {
        return;
    }
    if(index < GetSize() - 1){
        for (int i = index; i < GetSize() - 1;i ++) {
            array_[index].first = array_[index + 1].first;
            array_[index].second = array_[index + 1].second;
        }
    }
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirst() {
    memmove(array_, array_ + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveSecond() {
    memmove(array_ + 1, array_, GetSize()*sizeof(MappingType));
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetItem(const KeyType &key,const ValueType &val,int index) {
    assert(index < GetSize());
    array_[index].first = key;
    array_[index].second = val;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *target_leaf,bool mostLeft) {
    int start_index = target_leaf->GetSize();
    int offset = GetSize();
    if (mostLeft) {
        for(int index = 0; index < offset; index++) {
            target_leaf->array_[start_index + index].first = array_[index].first;
            target_leaf->array_[start_index + index].second = array_[index].second;
        }
    }else {
        for (int index = start_index - 1; index >= 0; index --) {
            target_leaf->array_[index + offset].first = target_leaf->array_[index].first;
            target_leaf->array_[index + offset].second = target_leaf->array_[index].second;
        }
        for (int index = 0; index < offset; index++) {
            target_leaf->array_[index].first = array_[index].first;
            target_leaf->array_[index].second = array_[index].second;
        }
    }
    target_leaf->SetNextPageId(GetNextPageId());
    target_leaf->IncreaseSize(GetSize());
    SetSize(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
