#include "replacer/clock_replacer.h"

/**
 * @brief 构造函数，初始化 CLOCK 替换器
 * @param num_pages 缓冲池的大小（帧的数量）
 */
ClockReplacer::ClockReplacer(size_t num_pages) 
    : frames_(num_pages), 
      clock_hand_(0), 
      max_size_(num_pages), 
      num_in_replacer_(0) {
    // frames_ 中的每个 ClockFrame 已经被默认构造函数初始化为 {ref=false, in_replacer=false}
}

ClockReplacer::~ClockReplacer() = default;

/**
 * @brief 使用 CLOCK 策略选择一个淘汰帧
 * 
 * 算法流程：
 * 1. 如果没有可淘汰的帧，返回 false
 * 2. 时钟指针开始转圈：
 *    - 跳过不在 replacer 中的帧（被 pin 住的）
 *    - 如果 ref=false，淘汰该帧
 *    - 如果 ref=true，给"第二次机会"，将 ref 置为 false，继续转
 * 
 * @param[out] frame_id 被淘汰的帧的 id
 * @return 成功返回 true，否则返回 false
 */
bool ClockReplacer::victim(frame_id_t* frame_id) {
    std::scoped_lock lock{latch_};
    
    // 如果没有可淘汰的帧，返回 false
    if (num_in_replacer_ == 0) {
        return false;
    }
    
    // 最多转两整圈（第一圈可能把所有 ref 置为 false，第二圈找到目标）
    size_t max_iterations = max_size_ * 2;
    
    for (size_t i = 0; i < max_iterations; i++) {
        ClockFrame& frame = frames_[clock_hand_];
        
        if (frame.in_replacer) {
            if (frame.ref == false) {
                // 找到了！淘汰这个帧
                *frame_id = static_cast<frame_id_t>(clock_hand_);
                frame.in_replacer = false;
                num_in_replacer_--;
                // 指针移动到下一个位置（为下次淘汰做准备）
                clock_hand_ = (clock_hand_ + 1) % max_size_;
                return true;
            } else {
                // 给"第二次机会"：ref 从 true 变成 false
                frame.ref = false;
            }
        }
        
        // 指针移动到下一个位置
        clock_hand_ = (clock_hand_ + 1) % max_size_;
    }
    
    // 理论上不会到这里，因为 num_in_replacer_ > 0 时一定能找到
    return false;
}

/**
 * @brief 固定一个帧，使其不能被淘汰
 * 
 * 当 BufferPoolManager 需要使用某个帧时调用此函数，
 * 表示"我正在用这个帧，别淘汰它"
 * 
 * @param frame_id 要固定的帧 id
 */
void ClockReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    
    // 边界检查
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_size_) {
        return;
    }
    
    ClockFrame& frame = frames_[frame_id];
    
    // 如果该帧在 replacer 中，将其移除
    if (frame.in_replacer) {
        frame.in_replacer = false;
        frame.ref = false;
        num_in_replacer_--;
    }
}

/**
 * @brief 取消固定一个帧，使其可以被淘汰
 * 
 * 当 BufferPoolManager 用完某个帧时调用此函数，
 * 表示"我用完了，这个帧可以被淘汰了"
 * 
 * @param frame_id 要取消固定的帧 id
 */
void ClockReplacer::unpin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    
    // 边界检查
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_size_) {
        return;
    }
    
    ClockFrame& frame = frames_[frame_id];
    
    // 如果已经在 replacer 中，不重复添加
    if (frame.in_replacer) {
        return;
    }
    
    // 加入 replacer，并设置 ref=true（刚被使用过，有"第一次机会"）
    frame.in_replacer = true;
    frame.ref = true;
    num_in_replacer_++;
}

/**
 * @brief 返回当前可以被淘汰的帧数量
 * @return 在 replacer 中的帧数量
 */
size_t ClockReplacer::Size() {
    std::scoped_lock lock{latch_};
    return num_in_replacer_;
}
