//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan) , child_executor_(std::move(child_executor)){
}

void InsertExecutor::Init() {
    child_executor_->Init();
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//    std::vector<Value> values {};
//    values.reserve(plan_->GetChildPlan()->OutputSchema().GetColumnCount());
//    std::vector<Value> values{};
//    auto &schema = plan_->GetChildPlan()->OutputSchema();

//    values.reserve(schema.GetColumnCount());
//    for (const auto &expr : plan_->Get) {
//
//    }
//    assert(child_executor_ != nullptr);
    RID new_rid;
    if (!child_executor_->Next(tuple,&new_rid)) {
        return false;
    }
    if (!table_info->table_->InsertTuple(*tuple,&new_rid,exec_ctx_->GetTransaction())) {
        return false;
    }
    std::vector<IndexInfo *> index = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto &i : index) {
        i->index_->InsertEntry(*tuple,new_rid,exec_ctx_->GetTransaction());
    }
    *rid = new_rid;
    return true;
}

}  // namespace bustub
