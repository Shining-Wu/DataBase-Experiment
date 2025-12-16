#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    std::map<TabCol, Value> make_feed_dict(RmRecord *rec);

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    //可能有问题的函数
    std::map<TabCol, Value> make_feed_dict(RmRecord *rec) override {
    std::map<TabCol, Value> feed_dict;

    const auto &left_cols = left_->cols();
    for (const auto &col : left_cols) {
        Value v;
        v.type = col.type;
        v.raw = std::make_shared<RmRecord>(col.len);
        memcpy(v.raw->data, rec->data + col.offset, col.len);

        feed_dict[{col.tab_name, col.name}] = v;
    }
    return feed_dict;
}


    void beginTuple() override {
        is_end_ = false;

        // 1. 初始化 outer table
        left_->beginTuple();
        left_rec_ = left_->Next();

        // 如果 outer table 为空，join 直接结束
        if (left_rec_ == nullptr) {
            is_end_ = true;
            return;
        }

        // 2. 用 outer tuple 的值 feed 给 inner table
        auto feed_dict = make_feed_dict(left_rec_.get(), left_->cols());
        right_->feed(feed_dict);

        // 3. 初始化 inner table
        right_->beginTuple();
    }

    void nextTuple() override {
        assert(!is_end());
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if( eval_conds( cols_, fed_conds_, rec.get()) )
                    break;
            }
            catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) return nullptr;

        while (true) {
            // 1. 尝试从 inner table 取一条
            auto right_rec = right_->Next();
            if (right_rec != nullptr) {
                // join 成功，拼接 record
                auto res = std::make_unique<RmRecord>(len_);
                memcpy(res->data, left_rec_->data, left_->tupleLen());
                memcpy(res->data + left_->tupleLen(),
                    right_rec->data,
                    right_->tupleLen());
                return res;
            }

            // 2. inner table 扫描结束，推进 outer table
            left_rec_ = left_->Next();
            if (left_rec_ == nullptr) {
                is_end_ = true;
                return nullptr;
            }

            // 3. 用新的 outer tuple feed inner table，并重新扫描
            auto feed_dict = make_feed_dict(left_rec_.get(), left_->cols());
            right_->feed(feed_dict);
            right_->beginTuple();
        }
    }

    Rid &rid() override { return _abstract_rid; }
};
