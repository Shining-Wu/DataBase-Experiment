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
        scan_ = std::make_unique<RmScan>(fh_);
        // 必须先调用 next() 才能让 scan 指向第一条记录或者 end
        // 注意：具体的 RmScan 实现可能不同，如果构造函数里已经指向第一个了，就不需要第一次 next
        // 假设 RmScan 构造后指向 Before-First，需要 next 才能到 First：
        scan_->next(); 
        
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            // Fix: 获取 unique_ptr 后要 get() 拿到裸指针传给 eval_conds
            auto rec = fh_->get_record(rid_, context_);
            // Fix: eval_conds 参数顺序通常是 (cols, conds, rec)
            if (check_conds(cols_, fed_conds_, rec.get())) {
                return; // Found match
            }
            scan_->next();
        }
        // Not found
        rid_ = Rid{RM_NO_PAGE, -1};
    }

    void nextTuple() override {
        if (rid_.page_no == RM_NO_PAGE) return;
        
        // 移动到下一条
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
        if (rid_.page_no == RM_NO_PAGE) {
            return nullptr;
        }
        // 1. 获取当前记录
        auto rec = fh_->get_record(rid_, context_);
        // 2. 移动游标寻找下一条满足条件的，为下一次 Next 做准备
        nextTuple();
        // 3. 返回当前记录
        return rec;
    }

    void feed(const std::map<TabCol, Value> &feed_dict) {
        fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                // 这是一个连接条件 (TableA.a = TableB.b)
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
