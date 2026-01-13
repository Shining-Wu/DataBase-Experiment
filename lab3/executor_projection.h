#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<size_t> sel_idxs_;

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        // Fix: 获取 prev_ 的 cols 需要用 ->
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    void beginTuple() override { prev_->beginTuple(); }

    void nextTuple() override { prev_->nextTuple(); }

    std::unique_ptr<RmRecord> Next() override {
        // Fix: 使用 prev_->Next()
        auto prev_rec = prev_->Next();
        // Fix: 空指针检查，如果子节点没数据了，投影也应该停止
        if (prev_rec == nullptr) {
            return nullptr;
        }

        auto &prev_cols = prev_->cols();
        auto proj_rec = std::make_unique<RmRecord>(len_);
        
        for (size_t proj_idx = 0; proj_idx < cols_.size(); proj_idx++) {
            size_t prev_idx = sel_idxs_[proj_idx]; // Fix: 使用 sel_idxs_
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = cols_[proj_idx];      // Fix: 使用 cols_

            memcpy(proj_rec->data + proj_col.offset, 
                   prev_rec->data + prev_col.offset, 
                   proj_col.len);
        }
        return proj_rec;
    }
    
    // 必须实现基类纯虚函数
    const std::vector<ColMeta> &cols() const override { return cols_; }
    
    // 投影算子通常不需要 feed，但作为中间节点需要传递给子节点（如果是在 Join 下方）
    void feed(const std::map<TabCol, Value> &feed_dict) override {
        prev_->feed(feed_dict);
    }

    Rid &rid() override { return _abstract_rid; }
};
