#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

    // 比较两个值
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

    // 根据比较操作符判断比较结果
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

    // 从记录中获取指定列的值
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
            // 去除末尾的空字符
            val.str_val.resize(strlen(val.str_val.c_str()));
        }
        return val;
    }

    // 查找列元数据
    const ColMeta *get_col_meta(const std::string &col_name) {
        for (const auto &col : cols_) {
            if (col.name == col_name) {
                return &col;
            }
        }
        return nullptr;
    }

    // 判断记录是否满足所有条件
    bool eval_conds(const RmRecord *rec) {
        for (const auto &cond : fed_conds_) {
            // 获取左侧列的值
            const ColMeta *lhs_col = get_col_meta(cond.lhs_col.col_name);
            if (lhs_col == nullptr) {
                continue;  // 列不存在，跳过
            }
            Value lhs_val = get_col_value(rec, *lhs_col);

            // 获取右侧的值
            Value rhs_val;
            if (cond.is_rhs_val) {
                rhs_val = cond.rhs_val;
            } else {
                const ColMeta *rhs_col = get_col_meta(cond.rhs_col.col_name);
                if (rhs_col == nullptr) {
                    continue;
                }
                rhs_val = get_col_value(rec, *rhs_col);
            }

            // 比较
            int cmp = compare_value(lhs_val, rhs_val);
            if (!check_cmp(cmp, cond.op)) {
                return false;
            }
        }
        return true;
    }

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 初始化扫描器
        scan_ = std::make_unique<RmScan>(fh_);

        // 从第一条记录开始扫描
        while (!scan_->is_end()) {
            Rid cur_rid = scan_->rid();
            try {
                auto rec = fh_->get_record(cur_rid, context_);  // 添加 context_ 参数
                if (eval_conds(rec.get())) {
                    rid_ = cur_rid;
                    return;
                }
            } catch (RecordNotFoundError &e) {
                // 记录不存在，继续扫描下一条
            }
            scan_->next();
        }

        // 扫描结束仍未找到
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    void nextTuple() override {
        if (rid_.page_no == RM_NO_PAGE) return;

        // 从当前记录的下一条开始扫
        scan_->next();

        while (!scan_->is_end()) {
            Rid cur_rid = scan_->rid();
            try {
                auto rec = fh_->get_record(cur_rid, context_);  // 添加 context_ 参数
                if (eval_conds(rec.get())) {
                    rid_ = cur_rid;
                    return;
                }
            } catch (RecordNotFoundError &e) {
                // 记录不存在，继续扫描下一条
            }
            scan_->next();
        }

        rid_ = Rid{RM_NO_PAGE, -1};
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rid_.page_no == RM_NO_PAGE) {
            return nullptr;
        }
        auto rec = fh_->get_record(rid_, context_);  // 添加 context_ 参数
        nextTuple();
        return rec;
    }

    void feed(const std::map<TabCol, Value> &feed_dict) {
        fed_conds_.clear();

        for (auto &cond : conds_) {
            // 如果右值是列（Join 条件）
            if (!cond.is_rhs_val) {
                auto it = feed_dict.find(cond.rhs_col);
                if (it != feed_dict.end()) {
                    // 用外表当前 tuple 的值替换 RHS
                    Condition new_cond = cond;
                    new_cond.is_rhs_val = true;
                    new_cond.rhs_val = it->second;
                    fed_conds_.push_back(new_cond);
                }
                // 如果 feed_dict 里没有这个列，跳过
            } else {
                // 原本就是常量条件，直接保留
                fed_conds_.push_back(cond);
            }
        }
    }

    bool is_end() const override {
        return rid_.page_no == RM_NO_PAGE;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    Rid &rid() override { return rid_; }
};