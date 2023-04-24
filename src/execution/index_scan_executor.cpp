//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {
}

void IndexScanExecutor::Init() {
    index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
    b_plus_tree_index = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
    index_item = b_plus_tree_index->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if ( index_item.IsEnd()) {
        return false;
    }
    auto &item = *index_item;
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
    table_info->table_->GetTuple(item.second,tuple,nullptr);
    *rid = item.second;
    ++index_item;
    return true;
}

}  // namespace bustub
