#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, bool rightMost) -> LeafPage * {
    if (IsEmpty()) {
      return nullptr;
    }
    page_id_t next = root_page_id_;
    auto page = buffer_pool_manager_->FetchPage(next);
    auto pointer = reinterpret_cast<BPlusTreePage *>(page->GetData());
    for (page_id_t cur = root_page_id_; !pointer->IsLeafPage(); cur = next,pointer = FetchPage(cur)) {
        auto *internal_page = static_cast<InternalPage *>(pointer);
        if (leftMost) {
            next = internal_page->ValueAt(0);
        } else if (rightMost) {
            next = internal_page->ValueAt(internal_page->GetSize()-1);
        } else {
            next = internal_page->Lookup(key, comparator_);
        }
        buffer_pool_manager_->UnpinPage(cur, false);
    }
    return static_cast<LeafPage *>(pointer);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    page->RLatch();
    return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    auto *tar = FindLeafPage(key);
    if (tar == nullptr) {
        return false;
    }
    result->resize(1);
    auto ret = tar->Lookup(key,result->at(0), comparator_);
    buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateRoot(const KeyType &key, const ValueType &value)  {
    page_id_t new_page_id;
    Page *root_page = buffer_pool_manager_->NewPage(&new_page_id);
    assert(root_page != nullptr);

    auto *root = reinterpret_cast<LeafPage *>(root_page->GetData());

    root->Init(new_page_id, INVALID_PAGE_ID);
    root_page_id_ = new_page_id;
    UpdateRootPageId(true);
    root->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(new_page_id, true);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
    LeafPage *leaf_page = FindLeafPage(key);
    ValueType v;
    bool exist = leaf_page->Lookup(key, v, comparator_);
    if (exist) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        return false;
    }
    leaf_page->Insert(key, value, comparator_);
    if (leaf_page->GetSize() > leaf_page->GetMaxSize()) {
        LeafPage *new_leaf_page = Split(leaf_page);
        InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                                      BPlusTreePage *new_node, Transaction *transaction) {
    if (old_node->IsRootPage()) {
        Page* const new_page = buffer_pool_manager_->NewPage(&root_page_id_);
        assert(new_page != nullptr);
        assert(new_page->GetPinCount() == 1);
        auto *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
        new_root->Init(root_page_id_);
        new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        UpdateRootPageId();
        //  fetch page and new page need to unpin page
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
        return;
    }
    page_id_t parent_id = old_node->GetParentPageId();
    auto *page = FetchPage(parent_id);
    assert(page != nullptr);
    auto *parent = reinterpret_cast<InternalPage *>(page);
    new_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (parent->GetSize() > parent->GetMaxSize()) {
        auto *new_leaf_page = Split(parent);
        InsertIntoParent(parent, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
    page_id_t new_page_id;
    Page* const new_page = buffer_pool_manager_->NewPage(&new_page_id);
    assert(new_page != nullptr);
    N *new_node = reinterpret_cast<N *>(new_page->GetData());
    new_node->Init(new_page_id, node->GetParentPageId());
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    if (IsEmpty()) {
        CreateRoot(key,value);
        return true;
    }
    return InsertIntoLeaf(key,value,transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (IsEmpty()) {
        return;
    }
    auto *delTar = FindLeafPage(key);
    int curSize = delTar->RemoveAndDeleteRecord(key, comparator_);
    if (curSize < delTar->GetMinSize()) {
        CoalesceOrRedistribute(delTar, transaction);
    }
    buffer_pool_manager_->UnpinPage(delTar->GetPageId(), true);
//    assert(Check());

//    auto *node = FindLeafPage(key);

//    if (node->GetSize() == node->RemoveAndDeleteRecord(key, comparator_)) {
//        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
//        return;
//    }

//    auto node_should_delete = CoalesceOrRedistribute(node, transaction);

//    if (node_should_delete) {
//        transaction->AddIntoDeletedPageSet(node->GetPageId());
//    }

//    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);

//    std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
//                  [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
//    transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
    if (node->IsRootPage()) {
        AdjustRoot(node);
        return true;
    }

    if (node->GetSize() >= node->GetMinSize()) {
        return false;
    }

    auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    auto idx = parent_node->ValueIndex(node->GetPageId());

    if (idx > 0) {
        auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx - 1));
//        sibling_page->WLatch();
        N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

        if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
            Redistribute(sibling_node, node, parent_node, idx, true);

            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            sibling_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
            return false;
        }

        // coalesce
        Coalesce(sibling_node, node, parent_node, idx, transaction);

//        if (parent_node_should_delete) {
//            transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
//        }
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
        return true;
    }

    if (idx != parent_node->GetSize() - 1) {
        auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx + 1));
        sibling_page->WLatch();
        N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

        if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
            Redistribute(sibling_node, node, parent_node, idx, false);

            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            sibling_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
            return false;
        }
        // coalesce
        auto sibling_idx = parent_node->ValueIndex(sibling_node->GetPageId());
        Coalesce(node, sibling_node, parent_node, sibling_idx, transaction);  // NOLINT
//        transaction->AddIntoDeletedPageSet(sibling_node->GetPageId());
//        if (parent_node_should_delete) {
//            transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
//        }
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
        return false;
    }

    return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N * &sibling) -> bool {
    auto page = FetchPage(node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(page);
    int index = parent->ValueIndex(node->GetPageId());
    int sibling_index = index - 1;
    if (index == 0) {
        sibling_index = index + 1;
    }
    sibling = reinterpret_cast<N *>(FetchPage(parent->ValueAt(sibling_index)));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return index == 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) -> bool {
    auto middle_key = parent->KeyAt(index);

    if (node->IsLeafPage()) {
        auto *leaf_node = reinterpret_cast<LeafPage *>(node);
        auto *prev_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
        leaf_node->MoveAllTo(prev_leaf_node);
    } else {
        auto *internal_node = reinterpret_cast<InternalPage *>(node);
        auto *prev_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
        internal_node->MoveAllTo(prev_internal_node, middle_key, buffer_pool_manager_);
    }

    parent->Remove(index);

    return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, InternalPage *parent, int index, bool from_prev) {
    if (node->IsLeafPage()) {
        auto *leaf_node = reinterpret_cast<LeafPage *>(node);
        auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

        if (!from_prev) {
            neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
            parent->SetKeyAt(index + 1, neighbor_leaf_node->KeyAt(0));
        } else {
            neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
            parent->SetKeyAt(index, leaf_node->KeyAt(0));
        }
    } else {
        auto *internal_node = reinterpret_cast<InternalPage *>(node);
        auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

        if (!from_prev) {
            neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index + 1), buffer_pool_manager_);
            parent->SetKeyAt(index + 1, neighbor_internal_node->KeyAt(0));
        } else {
            neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
            parent->SetKeyAt(index, internal_node->KeyAt(0));
        }
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsLeafPage()) {
        buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
        buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        return;
    }
    if (old_root_node->GetSize() == 1) {
        auto *root = reinterpret_cast<InternalPage *>(old_root_node);
        const page_id_t new_root_id = root->RemoveAndReturnOnlyChild();
        root_page_id_ = new_root_id;
        UpdateRootPageId();
        // set the new root's parent id "INVALID_PAGE_ID"
        Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
        assert(page != nullptr);
        auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
        new_root->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
        buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    if (IsEmpty()) {
        return INDEXITERATOR_TYPE(nullptr,0,buffer_pool_manager_);
    }
    KeyType useless;
    auto start_leaf = FindLeafPage(useless, true, false);
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    if (IsEmpty()) {
        return INDEXITERATOR_TYPE(nullptr,0,buffer_pool_manager_);
    }
    auto start_leaf = FindLeafPage(key);
    if (start_leaf == nullptr) {
        return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
    }
    int idx = start_leaf->KeyIndex(key, comparator_);
    return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
    if (root_page_id_ == INVALID_PAGE_ID) {
        return INDEXITERATOR_TYPE(nullptr, 0, nullptr);
    }
    auto *leaf_node = FindLeafPage(KeyType(),  false, true);
    return INDEXITERATOR_TYPE(leaf_node, leaf_node->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
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
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
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
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
