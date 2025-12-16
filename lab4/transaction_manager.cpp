#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (txn==nullptr) {
        txn_id_t new_txn_id=next_txn_id_++; 
        txn=new Transaction(new_txn_id);    
        txn->set_state(TransactionState::DEFAULT);  
    }
    std::unique_lock<std::mutex> lock(latch_);  
    txn_map[txn->get_transaction_id()]=txn;
    lock.unlock();
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (txn==nullptr) {
        return;
    }
    auto lock_set=txn->get_lock_set();
    for (auto &lock_data_id:*lock_set) {
        lock_manager_->unlock(txn,lock_data_id);
    }
    lock_set->clear();
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (txn==nullptr) {
        return;
    }
    auto write_set=txn->get_write_set();
    while (!write_set->empty()) {
        WriteRecord *write_record=write_set->back();
        write_set->pop_back();
        std::string tab_name=write_record->GetTableName();
        auto fh=sm_manager_->fhs_.at(tab_name).get();  
        WType wtype=write_record->GetWriteType();
        if (wtype==WType::INSERT_TUPLE) {
            fh->delete_record(write_record->GetRid(),nullptr);
        } 
        else if (wtype==WType::DELETE_TUPLE) {
            fh->insert_record(write_record->GetRid(),write_record->GetRecord().data);
        } 
        else if (wtype==WType::UPDATE_TUPLE) {
            fh->update_record(write_record->GetRid(),write_record->GetRecord().data,nullptr);
        }
        delete write_record;
    }
    auto lock_set=txn->get_lock_set();
    for (auto &lock_data_id:*lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    lock_set->clear();
    txn->set_state(TransactionState::ABORTED);    
}