#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // ===== 实验四：申请表级 IX 锁 =====
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        }

        // 获取所有相关的索引句柄
        std::vector<IxIndexHandle *> ihs(tab_.cols.size(), nullptr);
        
        // 对于每个 set_clause，检查对应列是否有索引
        for (auto &set_clause : set_clauses_) {
            auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
            if (lhs_col->index) {
                size_t lhs_col_idx = lhs_col - tab_.cols.begin();
                // 获取索引句柄
                std::vector<std::string> col_names = {lhs_col->name};
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, col_names);
                if (sm_manager_->ihs_.count(index_name)) {
                    ihs[lhs_col_idx] = sm_manager_->ihs_.at(index_name).get();
                }
            }
        }

        // 更新每条记录
        for (auto &rid : rids_) {
            // 1. 获取旧记录
            auto rec = fh_->get_record(rid, context_);
            
            // ===== 实验四：记录 WriteRecord 用于回滚（保存更新前的记录）=====
            if (context_ != nullptr && context_->txn_ != nullptr) {
                WriteRecord *write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
                context_->txn_->append_write_record(write_record);
            }
            
            // 2. 删除旧的索引项
            for (size_t i = 0; i < tab_.cols.size(); i++) {
                if (ihs[i] != nullptr) {
                    char *key = rec->data + tab_.cols[i].offset;
                    ihs[i]->delete_entry(key, context_->txn_);
                }
            }

            // 3. 构造新记录
            RmRecord update_record(rec->size);
            memcpy(update_record.data, rec->data, rec->size);
            
            // 应用 set_clauses 中的更新
            for (auto &set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                memcpy(update_record.data + lhs_col->offset, 
                       set_clause.rhs.raw->data, 
                       lhs_col->len);
            }

            // 4. 更新记录文件
            fh_->update_record(rid, update_record.data, context_);

            // 5. 插入新的索引项
            for (size_t i = 0; i < tab_.cols.size(); i++) {
                if (ihs[i] != nullptr) {
                    char *key = update_record.data + tab_.cols[i].offset;
                    ihs[i]->insert_entry(key, rid, context_->txn_);
                }
            }
        }
        
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};