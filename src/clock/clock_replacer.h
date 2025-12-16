#pragma once

#include <mutex>
#include <vector>

#include "common/config.h"
#include "replacer/replacer.h"

/**
 * ClockReplacer 实现了 CLOCK 替换策略
 * 
 * CLOCK 是 LRU 的近似算法，也叫"第二次机会"算法：
 * - 每个帧有一个引用位(ref)，被访问时设为 true
 * - 淘汰时，时钟指针转圈寻找 ref=false 的帧
 * - 如果 ref=true，给"第二次机会"，将 ref 置为 false，继续找下一个
 * 
 * 优点：比 LRU 实现简单，每次访问开销更小（只设置一个 bit）
 * 缺点：淘汰时最坏情况需要转一圈 O(n)
 * 
 * 实际数据库（如 PostgreSQL）广泛使用 CLOCK 及其变种
 */
class ClockReplacer : public Replacer {
   public:
    /**
     * @brief 创建一个新的 ClockReplacer
     * @param num_pages 最多需要管理的帧数量
     */
    explicit ClockReplacer(size_t num_pages);

    ~ClockReplacer();

    /**
     * @brief 使用 CLOCK 策略选择一个淘汰帧
     * @param[out] frame_id 被淘汰的帧的 id
     * @return 如果成功找到淘汰帧返回 true，否则返回 false
     */
    bool victim(frame_id_t *frame_id) override;

    /**
     * @brief 固定一个帧（该帧正在使用，不能被淘汰）
     * @param frame_id 要固定的帧 id
     */
    void pin(frame_id_t frame_id) override;

    /**
     * @brief 取消固定一个帧（该帧可以被淘汰了）
     * @param frame_id 要取消固定的帧 id
     */
    void unpin(frame_id_t frame_id) override;

    /**
     * @brief 返回当前可以被淘汰的帧数量
     */
    size_t Size() override;

   private:
    /**
     * @brief 每个帧的状态
     */
    struct ClockFrame {
        bool ref;          // 引用位：最近是否被访问过
        bool in_replacer;  // 是否在 replacer 中（可被淘汰）
        
        ClockFrame() : ref(false), in_replacer(false) {}
    };

    std::mutex latch_;                 // 互斥锁，保证线程安全
    std::vector<ClockFrame> frames_;   // 所有帧的状态数组
    size_t clock_hand_;                // 时钟指针，指向当前检查位置
    size_t max_size_;                  // 最大容量
    size_t num_in_replacer_;           // 当前在 replacer 中的帧数量
};
