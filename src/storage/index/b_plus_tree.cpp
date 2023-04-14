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
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,bool leftMost,bool rightMost) -> LeafPage * {
    if (IsEmpty()) return nullptr;
    auto pointer = FetchPage(root_page_id_);
    page_id_t next;
    for (page_id_t cur = root_page_id_; !pointer->IsLeafPage(); cur = next,pointer = FetchPage(cur)) {
        auto *internalPage = static_cast<InternalPage *>(pointer);
        if (leftMost) {
            next = internalPage->ValueAt(0);
        }else if(rightMost){
            next = internalPage->ValueAt(internalPage->GetSize()-1);
        }else {
            next = internalPage->Lookup(key,comparator_);
        }
        buffer_pool_manager_->UnpinPage(cur,false);
    }
    return static_cast<LeafPage *>(pointer);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
    //step 1. find page
    B_PLUS_TREE_LEAF_PAGE_TYPE *tar = FindLeafPage(key);
    //step 2. find value
    result->resize(1);
    auto ret = tar->Lookup(key,result->at(0),comparator_);
    //step 3. unPin buffer pool
    buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateRoot(const KeyType &key,const ValueType &value)  {
//    page_id_t newPageId;
//    Page *rootPage = buffer_pool_manager_->NewPage(&newPageId);
//    assert(rootPage != nullptr);
//
//    auto *root = reinterpret_cast<LeafPage *>(rootPage->GetData());
//
//    root->Init(newPageId,INVALID_PAGE_ID);
//    root_page_id_ = newPageId;
//    UpdateRootPageId(true);
//    root->Insert(key,value,comparator_);
//
//    buffer_pool_manager_->UnpinPage(newPageId,true);
//
    //step 1. ask for new page from buffer pool manager
    page_id_t newPageId;
    Page *rootPage = buffer_pool_manager_->NewPage(&newPageId);
    assert(rootPage != nullptr);

    auto *root = reinterpret_cast<LeafPage *>(rootPage->GetData());

    //step 2. update b+ tree's root page id
    root->Init(newPageId,INVALID_PAGE_ID);
    root_page_id_ = newPageId;
    UpdateRootPageId(true);
    //step 3. insert entry directly into leaf page.
    root->Insert(key,value,comparator_);

    buffer_pool_manager_->UnpinPage(newPageId,true);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
    LeafPage *leafPage = FindLeafPage(key);
    ValueType v;
    bool exist = leafPage->Lookup(key,v,comparator_);
    if (exist) {
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
        return false;
    }
    leafPage->Insert(key,value,comparator_);
    if (leafPage->GetSize() > leafPage->GetMaxSize()) {//insert then split
        LeafPage *newLeafPage = Split(leafPage);//unpin it in below func
        InsertIntoParent(leafPage,newLeafPage->KeyAt(0),newLeafPage,transaction);
    }
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,const KeyType &key,BPlusTreePage *new_node,Transaction *transaction) {
    if (old_node->IsRootPage()) {
        Page* const newPage = buffer_pool_manager_->NewPage(&root_page_id_);
        assert(newPage != nullptr);
        assert(newPage->GetPinCount() == 1);
        auto *newRoot = reinterpret_cast<InternalPage *>(newPage->GetData());
        newRoot->Init(root_page_id_);
        newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        UpdateRootPageId();
        //fetch page and new page need to unpin page
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(newRoot->GetPageId(),true);
        return;
    }
    page_id_t parentId = old_node->GetParentPageId();
    auto *page = FetchPage(parentId);
    assert(page != nullptr);
    auto *parent = reinterpret_cast<InternalPage *>(page);
    new_node->SetParentPageId(parentId);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    //insert new node after old node
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (parent->GetSize() > parent->GetMaxSize()) {
        //begin /* Split Parent */
        auto *newLeafPage = Split(parent);//new page need unpin
        InsertIntoParent(parent,newLeafPage->KeyAt(0),newLeafPage,transaction);
    }
    buffer_pool_manager_->UnpinPage(parentId,true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t newPageId;
    Page* const newPage = buffer_pool_manager_->NewPage(&newPageId);
    assert(newPage != nullptr);
    N *newNode = reinterpret_cast<N *>(newPage->GetData());
    newNode->Init(newPageId, node->GetParentPageId());
    node->MoveHalfTo(newNode, buffer_pool_manager_);
    return newNode;
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
    if(IsEmpty()){
        return;
    }
    auto *lp = FindLeafPage(key);
    int index = lp->KeyIndexPrecise(key,comparator_);
    if (index == -1) {
        return;
    }
    lp->Remove(key,comparator_);

    if (lp->GetSize() < lp->GetMinSize()) {
        // merge node
        RemoveMerge(lp);
    }
    buffer_pool_manager_->UnpinPage(lp->GetPageId(),true);
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveMerge (BPlusTreePage *p) {
    if (p->IsRootPage()) {
        AdjustRoot(p);
        return;
    }
    auto *pp = buffer_pool_manager_->FetchPage(p->GetParentPageId());

    auto *p_node = reinterpret_cast<BPlusTreePage *>(pp->GetData());

    BPlusTreePage sibling_node{} ;
    int sibling_index = FindSibling(p,&sibling_node);

    auto *p_inter_node = reinterpret_cast<InternalPage*>(pp->GetData());

    if (p->GetSize() + sibling_node.GetSize() <= p->GetMaxSize()) {
//        std::swap(p,sibling_node);
        int n_index = p_inter_node->ValueIndex(p->GetPageId());
        Coalesce(&sibling_node,p,p_inter_node,n_index,sibling_index == 0);
        buffer_pool_manager_->UnpinPage(p->GetParentPageId(),true);
        return;
    }
    int nodeInParentIndex = p_inter_node->ValueIndex(p->GetPageId());
    Redistribute(&sibling_node,p,nodeInParentIndex);//unpin node,node2
    buffer_pool_manager_->UnpinPage(p_inter_node->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindSibling(BPlusTreePage *node,BPlusTreePage *sibling_node_out) -> int {
    auto *p = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *lp = reinterpret_cast<InternalPage *>(p->GetData());

    int index = lp->ValueIndex(node->GetPageId());
    int sibling_index = index - 1;
    if (index == 0) {
        sibling_index = index + 1;
    }
    auto *sb = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(lp->ValueAt(sibling_index))->GetData());
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(),false);
    *sibling_node_out = *sb;
    return index == 0 ? 1 : 0;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *p) {
    if (p->IsLeafPage()) {
        assert(p->GetSize() == 0);
        assert (p->GetParentPageId() == INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
        buffer_pool_manager_->DeletePage(p->GetPageId());
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        return;
    }
    if (p->GetSize() == 1) {
        auto *lp = reinterpret_cast<InternalPage *>(p);
        page_id_t newRootId = lp->RemoveAndReturn();
        root_page_id_ = newRootId;
        UpdateRootPageId();
        auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
        auto *n_lp = reinterpret_cast<InternalPage*>(page->GetData());
        n_lp->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        buffer_pool_manager_->UnpinPage(p->GetPageId(),false);
        buffer_pool_manager_->DeletePage(p->GetPageId());
    }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(BPlusTreePage *sibing_node,BPlusTreePage *node,InternalPage *parent_node,int node_index,bool isLeft) {
    assert(sibing_node->GetPageId() + node->GetSize() <= node->GetMaxSize());
    if (node->IsLeafPage()) {
        auto *node_type_ptr = reinterpret_cast<LeafPage *>(node);
        node_type_ptr->MoveAllTo(reinterpret_cast<LeafPage *>(sibing_node),isLeft);
    }else {
        auto *node_type_ptr = reinterpret_cast<InternalPage *>(node);
        node_type_ptr->MoveAllTo(reinterpret_cast<InternalPage *>(sibing_node),isLeft,buffer_pool_manager_);
    }

    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(sibing_node->GetPageId(),true);
    buffer_pool_manager_->DeletePage(node->GetPageId());
    parent_node->Remove(node_index);
    if (parent_node->GetSize() <= parent_node->GetMinSize()) {
        RemoveMerge(parent_node);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *sibling_node,BPlusTreePage *node,int nodeInParentIndex) {
//    if (nodeInParentIndex == 0) {
//        MoveFirstToEnd(sibling_node,node);
//    } else {
//        MoveLastToFront(sibling_node,node,nodeInParentIndex);
//    }
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
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
    KeyType useless;
    auto start_leaf = FindLeafPage(useless, true,false);
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    auto start_leaf = FindLeafPage(key);
    if (start_leaf == nullptr) {
        return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
    }
    int idx = start_leaf->KeyIndex(key,comparator_);
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
    return INDEXITERATOR_TYPE(leaf_node,leaf_node->GetSize() ,buffer_pool_manager_);
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
