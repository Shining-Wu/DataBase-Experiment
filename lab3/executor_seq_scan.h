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
            auto rec = fh_->get_record(cur_rid);

            // 判断是否满足条件（注意是 fed_conds_）
            if (eval_conds(rec.get(), fed_conds_, cols_)) {
                rid_ = cur_rid;
                return;
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
            auto rec = fh_->get_record(cur_rid);

            if (eval_conds(rec.get(), fed_conds_, cols_)) {
                rid_ = cur_rid;
                return;
            }
            scan_->next();
        }

        rid_ = Rid{RM_NO_PAGE, -1};
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rid_.page_no == RM_NO_PAGE) {
            return nullptr;
        }
        auto rec = fh_->get_record(rid_);
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

    Rid &rid() override { return rid_; }
};
