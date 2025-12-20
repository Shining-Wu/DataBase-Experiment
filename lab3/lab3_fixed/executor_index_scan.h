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

    // ============ 辅助函数：条件判断 ============
    int compare_value(const Value &lhs, const Value &rhs) {
        if (lhs.type == TYPE_INT && rhs.type == TYPE_INT) {
            return lhs.int_val - rhs.int_val;
        } else if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_FLOAT) {
            if (lhs.float_val < rhs.float_val) return -1;
            if (lhs.float_val > rhs.float_val) return 1;
            return 0;
        } else if (lhs.type == TYPE_STRING && rhs.type == TYPE_STRING) {
            return lhs.str_val.compare(rhs.str_val);
        } else if (lhs.type == TYPE_INT && rhs.type == TYPE_FLOAT) {
            float lhs_f = static_cast<float>(lhs.int_val);
            if (lhs_f < rhs.float_val) return -1;
            if (lhs_f > rhs.float_val) return 1;
            return 0;
        } else if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_INT) {
            float rhs_f = static_cast<float>(rhs.int_val);
            if (lhs.float_val < rhs_f) return -1;
            if (lhs.float_val > rhs_f) return 1;
            return 0;
        }
        return 0;
    }

    bool check_cmp(int cmp, CompOp op) {
        switch (op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default: return false;
        }
    }

    Value get_col_value(const RmRecord *rec, const ColMeta &col) {
        Value val;
        val.type = col.type;
        char *data = rec->data + col.offset;
        if (col.type == TYPE_INT) {
            val.int_val = *(int *)data;
        } else if (col.type == TYPE_FLOAT) {
            val.float_val = *(float *)data;
        } else if (col.type == TYPE_STRING) {
            val.str_val = std::string(data, col.len);
            val.str_val.resize(strlen(val.str_val.c_str()));
        }
        return val;
    }

    const ColMeta *get_col_meta(const std::string &col_name) {
        for (const auto &col : cols_) {
            if (col.name == col_name) {
                return &col;
            }
        }
        return nullptr;
    }

    bool eval_conds(const RmRecord *rec) {
        for (const auto &cond : fed_conds_) {
            const ColMeta *lhs_col = get_col_meta(cond.lhs_col.col_name);
            if (lhs_col == nullptr) continue;
            Value lhs_val = get_col_value(rec, *lhs_col);

            Value rhs_val;
            if (cond.is_rhs_val) {
                rhs_val = cond.rhs_val;
            } else {
                const ColMeta *rhs_col = get_col_meta(cond.rhs_col.col_name);
                if (rhs_col == nullptr) continue;
                rhs_val = get_col_value(rec, *rhs_col);
            }

            int cmp = compare_value(lhs_val, rhs_val);
            if (!check_cmp(cmp, cond.op)) {
                return false;
            }
        }
        return true;
    }
    // ============ 辅助函数结束 ============

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, 
                      std::vector<std::string> index_col_names, Context *context) {
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
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 1. 获取索引句柄
        auto ih = sm_manager_->ihs_
            .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_))
            .get();

        // 2. 默认扫描整个索引
        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();

        // 3. 根据 fed_conds_ 缩小索引范围
        // 找到索引列对应的 ColMeta
        const ColMeta *index_col = nullptr;
        if (!index_col_names_.empty()) {
            index_col = get_col_meta(index_col_names_[0]);
        }

        if (index_col != nullptr) {
            for (auto &cond : fed_conds_) {
                if (!cond.is_rhs_val) continue;
                if (cond.op == OP_NE) continue;
                if (cond.lhs_col.col_name != index_col->name) continue;

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
        }

        // 4. 构造索引扫描器
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());

        // 5. 找第一条满足所有条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(rec.get())) {
                    return;
                }
            } catch (RecordNotFoundError &e) {
                // 记录不存在，继续
            }
            scan_->next();
        }
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(rec.get())) {
                    return;
                }
            } catch (RecordNotFoundError &e) {
                // 记录不存在，继续
            }
            scan_->next();
        }
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    bool is_end() const override {
        return rid_.page_no == RM_NO_PAGE;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    void feed(const std::map<TabCol, Value> &feed_dict) {
        fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && feed_dict.count(cond.rhs_col)) {
                cond.rhs_val = feed_dict.at(cond.rhs_col);
                cond.is_rhs_val = true;
            }
        }
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    Rid &rid() override { return rid_; }
};