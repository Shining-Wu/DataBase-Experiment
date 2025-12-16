#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 1. 检查事务状态，SHRINKING 阶段不能申请锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建行级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查当前事务是否已经持有这个锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            // 已经持有锁，直接返回成功
            return true;
        }
    }
    
    // 6. 检查锁相容性（S锁与X锁不相容）
    // 如果队列中有 X 锁，则不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::X) {
        // no-wait 策略：直接抛出异常，终止事务
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求并加入队列
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新队列的 GroupLockMode
    // S 锁比 IS、IX 强，但比 X 弱
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }
    // 如果已经是 S，保持 S
    // 如果是 IX，变成 SIX（但行级锁不会出现这种情况）
    
    // 9. 将锁加入事务的锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态为 GROWING（可以继续申请锁）
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建行级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查当前事务是否已经持有这个锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            // 如果已经持有 X 锁，直接返回
            if (request.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;
            }
            // 如果持有 S 锁，需要升级（简化处理：如果只有自己一个 S 锁可以升级）
            // 这里简化为不支持升级，返回 true
            return true;
        }
    }
    
    // 6. 检查锁相容性（X锁与所有锁都不相容）
    if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        // no-wait 策略：有任何其他锁都不相容
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求并加入队列
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新队列的 GroupLockMode 为 X
    request_queue.group_lock_mode_ = GroupLockMode::X;
    
    // 9. 将锁加入事务的锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建表级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查是否已持有锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            return true;
        }
    }
    
    // 6. 检查锁相容性
    // S 锁与 IX、SIX、X 不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::IX ||
        request_queue.group_lock_mode_ == GroupLockMode::SIX ||
        request_queue.group_lock_mode_ == GroupLockMode::X) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新 GroupLockMode
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }
    
    // 9. 加入事务锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建表级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查是否已持有锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            if (request.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;
            }
        }
    }
    
    // 6. 检查锁相容性（X 锁与所有锁都不相容）
    if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新 GroupLockMode
    request_queue.group_lock_mode_ = GroupLockMode::X;
    
    // 9. 加入事务锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建表级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查是否已持有锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            return true;
        }
    }
    
    // 6. 检查锁相容性
    // IS 锁只与 X 锁不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::X) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新 GroupLockMode（IS 是最弱的锁，只有当前是 NON_LOCK 才更新）
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        request_queue.group_lock_mode_ = GroupLockMode::IS;
    }
    
    // 9. 加入事务锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 2. 创建表级锁的 LockDataId
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // 3. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 4. 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // 5. 检查是否已持有锁
    for (auto& request : request_queue.request_queue_) {
        if (request.txn_id_ == txn->get_transaction_id() && request.granted_) {
            return true;
        }
    }
    
    // 6. 检查锁相容性
    // IX 锁与 S、SIX、X 不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::S ||
        request_queue.group_lock_mode_ == GroupLockMode::SIX ||
        request_queue.group_lock_mode_ == GroupLockMode::X) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 7. 创建锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // 8. 更新 GroupLockMode
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::IX;
    }
    // 如果当前是 IX，保持 IX
    
    // 9. 加入事务锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    // 10. 更新事务状态
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    // 1. 加锁保护全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    
    // 2. 检查锁是否存在
    if (lock_table_.find(lock_data_id) == lock_table_.end()) {
        return false;
    }
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // 3. 在队列中找到该事务的锁请求并移除
    auto it = request_queue.request_queue_.begin();
    while (it != request_queue.request_queue_.end()) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            it = request_queue.request_queue_.erase(it);
            break;  // 一个事务对一个数据项只有一个锁
        } else {
            ++it;
        }
    }
    
    // 4. 重新计算队列的 GroupLockMode
    request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
    for (auto& request : request_queue.request_queue_) {
        if (request.granted_) {
            GroupLockMode mode = GroupLockMode::NON_LOCK;
            switch (request.lock_mode_) {
                case LockMode::INTENTION_SHARED:
                    mode = GroupLockMode::IS;
                    break;
                case LockMode::INTENTION_EXCLUSIVE:
                    mode = GroupLockMode::IX;
                    break;
                case LockMode::SHARED:
                    mode = GroupLockMode::S;
                    break;
                case LockMode::EXLUCSIVE:
                    mode = GroupLockMode::X;
                    break;
                case LockMode::S_IX:
                    mode = GroupLockMode::SIX;
                    break;
            }
            // 取最强的锁模式
            if (mode > request_queue.group_lock_mode_) {
                request_queue.group_lock_mode_ = mode;
            }
        }
    }
    
    // 5. 更新事务状态为 SHRINKING（进入收缩阶段，不能再申请锁）
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }
    
    return true;
}
