#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 1. 获取索引句柄
        auto ih = sm_manager_->ihs_
            .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_no_))
            .get();

        // 2. 默认扫描整个索引
        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();

        // 3. 根据 fed_conds_ 缩小索引范围（只取第一个可用索引条件）
        auto &index_col = cols_[index_no_];
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val) continue;
            if (cond.op == OP_NE) continue;
            if (cond.lhs_col.col_name != index_col.name) continue;

            char *rhs_key = cond.rhs_val.raw->data;

            if (cond.op == OP_EQ) {
                lower = ih->lower_bound(rhs_key);
                upper = ih->upper_bound(rhs_key);
            } else if (cond.op == OP_GE) {
                lower = ih->lower_bound(rhs_key);
            } else if (cond.op == OP_GT) {
                lower = ih->upper_bound(rhs_key);
            } else if (cond.op == OP_LE) {
                upper = ih->upper_bound(rhs_key);
            } else if (cond.op == OP_LT) {
                upper = ih->lower_bound(rhs_key);
            }
            break;
        }

        // 4. 构造索引扫描器
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());

        // 5. 找第一条满足所有条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return fh_->get_record(rid_, context_);
    }

    void feed(const std::map<TabCol, Value> &feed_dict) override {
        fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && feed_dict.count(cond.rhs_col)) {
                cond.rhs_val = feed_dict.at(cond.rhs_col);
                cond.is_rhs_val = true;
            }
        }
    }

    Rid &rid() override { return rid_; }
};
