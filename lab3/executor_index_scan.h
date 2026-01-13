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
        // 1. 获取索引句柄 (注意命名规则，这里假设 get_ix_manager 接口存在)
        // 通常索引文件名类似于 "tab_name_col_name_idx"
        auto ih = sm_manager_->ihs_.at(
            sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)
        ).get();

        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();

        // 2. 简单逻辑：只支持单列索引的最左前缀匹配
        // 找到第一个能利用索引的条件
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val) continue; 
            if (cond.lhs_col.col_name != index_col_names_[0]) continue; // 不是索引列
            if (cond.op == OP_NE) continue; // 不等号无法优化

            // Fix: Value 结构转换
            char *key = cond.rhs_val.raw->data; // 假设 Value 内部布局支持这样访问

            if (cond.op == OP_EQ) {
                lower = ih->lower_bound(key);
                upper = ih->upper_bound(key);
            } else if (cond.op == OP_GE) {
                lower = ih->lower_bound(key);
            } else if (cond.op == OP_GT) {
                lower = ih->upper_bound(key); // 大于通常用 upper_bound 开始
            } else if (cond.op == OP_LE) {
                upper = ih->upper_bound(key);
            } else if (cond.op == OP_LT) {
                upper = ih->lower_bound(key);
            }
            break; // 本实验通常只处理一个索引条件
        }

        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());

        // 寻找第一个有效元组
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (check_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    void nextTuple() override {
       if (rid_.page_no == RM_NO_PAGE) return;
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (check_conds(cols_, fed_conds_, rec.get())) {
                return;
            }
            scan_->next();
        }
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rid_.page_no == RM_NO_PAGE) return nullptr;
        auto rec = fh_->get_record(rid_, context_);
        nextTuple();
        return rec;
    }

    void feed(const std::map<TabCol, Value> &feed_dict)  {
        fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                auto it = feed_dict.find(cond.rhs_col);
                if (it != feed_dict.end()) {
                    cond.is_rhs_val = true;
                    cond.rhs_val = it->second;
                }
            }
        }
    }

    Rid &rid() override { return rid_; }
};
