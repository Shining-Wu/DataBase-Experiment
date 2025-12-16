# 实验一：CLOCK页面替换算法实现 - 修改说明文档

## 概述
本文档记录了在缓冲池管理器实验中实现CLOCK替换算法所进行的具体代码修改。

## 修改文件列表
本次修改涉及以下源代码文件，它们均位于 src/ 目录下的相应位置：

- 文件：common/config.h
  修改内容：修改替换算法选择宏（第42行附近），实现LRU/CLOCK全局切换。

- 文件：storage/buffer_pool_manager.h
  修改内容：修改BufferPoolManager类，增加对ClockReplacer的支持。

- 文件：replacer/clock_replacer.h
  修改内容：新增，CLOCK替换算法的头文件。

- 文件：replacer/clock_replacer.cpp
  修改内容：新增，CLOCK替换算法的具体实现。

- 文件：storage/CMakeLists.txt
  修改内容：将新增的clock_replacer.cpp源文件加入编译系统。

## 详细说明
关于每项修改的详细原因和截图，请参阅实验一报告

## 如何验证与切换算法
1. 打开 src/common/config.h 文件。
2. 找到替换算法选择宏（约第42行），将其值从 "LRU" 改为 "CLOCK"。
3. 保存文件，重新编译并运行测试，即可验证CLOCK算法是否生效。
