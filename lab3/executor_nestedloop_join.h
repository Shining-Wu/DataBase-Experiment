#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;   // outer
    std::unique_ptr<AbstractExecutor> right_;  // inner

    std::unique_ptr<RmRecord> left_rec_;       // 当前 outer tuple
    std::unique_ptr<RmRecord> cur_rec_;        // 当前 join 结果
    bool is_end_;

    size_t len_;
    std::vector<ColMeta> cols_;

    Rid rid_;

    std::map<TabCol, Value> make_feed_dict(RmRecord *rec) {
        std::map<TabCol, Value> feed_dict;
        for (auto &col : left_->cols()) {
            Value v;
            v.type = col.type;
            v.raw = std::make_shared<RmRecord>(col.len);
            memcpy(v.raw->data, rec->data + col.offset, col.len);
            feed_dict[{col.tab_name, col.name}] = v;
        }
        return feed_dict;
    }

    // 内部：推进到下一个 join tuple
    void find_next() {
        cur_rec_.reset();

        while (true) {
            auto right_rec = right_->Next();
            if (right_rec != nullptr) {
                cur_rec_ = std::make_unique<RmRecord>(len_);
                memcpy(cur_rec_->data, left_rec_->data, left_->tupleLen());
                memcpy(cur_rec_->data + left_->tupleLen(),
                       right_rec->data,
                       right_->tupleLen());
                return;
            }

            // inner exhausted → advance outer
            left_rec_ = left_->Next();
            if (left_rec_ == nullptr) {
                is_end_ = true;
                return;
            }

            auto feed_dict = make_feed_dict(left_rec_.get());
            right_->feed(feed_dict);
            right_->beginTuple();
        }
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                           std::unique_ptr<AbstractExecutor> right)
        : left_(std::move(left)),
          right_(std::move(right)),
          is_end_(false) {

        rid_={0,0};

        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
    }

    Rid &rid() override{return rid_;}

    void beginTuple() override {
        is_end_ = false;
        left_->beginTuple();
        left_rec_ = left_->Next();

        if (left_rec_ == nullptr) {
            is_end_ = true;
            return;
        }

        auto feed_dict = make_feed_dict(left_rec_.get());
        right_->feed(feed_dict);
        right_->beginTuple();

        find_next();  // 定位第一个 join 结果
    }

    void nextTuple() override {
        if (is_end_) return;
        find_next();
    }

    std::unique_ptr<RmRecord> Next() override {
       if (is_end_) return nullptr;
        auto ret = std::make_unique<RmRecord>(*cur_rec_);
        nextTuple(); // 移动到下一条
        return ret;
    }

    bool is_end() const override { return is_end_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
};
