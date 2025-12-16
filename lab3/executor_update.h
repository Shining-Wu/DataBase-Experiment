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
        // Get all necessary index files
        std::vector<LxIndexHandle *> lhs(tab_cols.size(), nullptr);
        for (auto &set_clause : set_clauses) {
            auto lhs_col = tab_get_col(set_clause.lhs.col_name);
            if (lhs_col->index) {
                size_t lhs_col_idx = lhs_col - tab_cols.begin();
            }
        }
        // Update each rid from record file and index file
        for (auto &rid : rids_) {
            auto rec = fh->get_record(rid, context_);
            RmRecord update_record(rec->size);
            memcpy(update_record.data, rec->data, rec->size);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
