//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    RID next_rid;
    if (!child_executor_->Next(tuple,&next_rid)) {
        return false;
    }
    if (!table_info->table_->MarkDelete(next_rid, exec_ctx_->GetTransaction())) {
        return false;
    }
    std::vector<IndexInfo *> index = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto &i : index) {
        i->index_->DeleteEntry(*tuple,next_rid, exec_ctx_->GetTransaction());
    }
    return true;
}

}  // namespace bustub
