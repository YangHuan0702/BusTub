//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
    AbstractExecutor(exec_ctx),plan_(plan),table_info(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)),
    target_iterator(table_info->table_->Begin(nullptr)){
}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    target_iterator++;
    if (target_iterator == table_info->table_->End()) {
        return false;
    }
    *tuple = *target_iterator;
    *rid = target_iterator->GetRid();
    return true;
}

}  // namespace bustub
