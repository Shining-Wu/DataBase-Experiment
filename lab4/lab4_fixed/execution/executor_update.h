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
        for (auto &rid : rids_) {
            // 1. 获取旧记录
            auto rec = fh_->get_record(rid, context_);
            
            // 2. 构造新记录（先拷贝旧的）
            RmRecord new_rec(fh_->get_file_hdr().record_size);
            memcpy(new_rec.data, rec->data, new_rec.size);

            // 3. 应用 SET 子句更新新记录的数据
            for (auto &set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                // Value 拷贝到 new_rec
                memcpy(new_rec.data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }

            // 4. 更新索引 (策略：删除旧 Key，插入新 Key)
            // 优化：其实只有当索引列被修改时才需要动索引，但为了简单，这里全部重做
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
                ).get();

                // --- 删除旧索引 ---
                char *old_key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(old_key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->delete_entry(old_key, context_->txn_);
                delete[] old_key;

                // --- 插入新索引 ---
                char *new_key = new char[index.col_tot_len];
                offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(new_key + offset, new_rec.data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                // 注意：这里仍然使用相同的 rid，因为我们是在原位 update_record
                ih->insert_entry(new_key, rid, context_->txn_);
                delete[] new_key;
            }

            // 5. 更新数据文件
            fh_->update_record(rid, new_rec.data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
