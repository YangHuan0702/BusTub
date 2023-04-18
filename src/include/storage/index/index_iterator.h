//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once

#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

    INDEX_TEMPLATE_ARGUMENTS
    class IndexIterator {
    public:
        IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager);

        IndexIterator();

        ~IndexIterator();// NOLINT

        auto IsEnd() -> bool;

        auto operator*() -> const MappingType &;

        auto operator++() -> IndexIterator &;

        auto operator==(const IndexIterator &itr) const -> bool {
            return leaf_ == itr.leaf_ && index_ == itr.index_;
        }

        auto operator!=(const IndexIterator &itr) const -> bool {
            return !this->operator==(itr);
        }

    private:
        // add your own private member variables here
        int index_;
        B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
        BufferPoolManager *buffer_pool_manager_;
    };

}// namespace bustub
