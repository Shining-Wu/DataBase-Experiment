#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
    }
    // 初始化索引
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        for (auto &col : tab.cols) {
            if (col.index) {
                std::vector<std::string> col_names = {col.name};
                auto ih = ix_manager_->open_index(tab.name, col_names);
                ihs_.emplace(ix_manager_->get_index_name(tab.name, col_names), std::move(ih));
            }
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();
    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();
    ihs_.clear();
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // ===== 实验四：申请表级 X 锁（防御性空指针检查）=====
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        auto fh = fhs_.at(tab_name).get();
        context->lock_mgr_->lock_exclusive_on_table(context->txn_, fh->GetFd());
    }

    TabMeta &tab = db_.tabs_[tab_name];

    // 1. 删除索引文件
    for (auto &col : tab.cols) {
        if (col.index) {
            std::vector<std::string> col_names = {col.name};
            auto index_name = ix_manager_->get_index_name(tab.name, col_names);
            ihs_.erase(index_name);
            ix_manager_->destroy_index(tab.name, col_names);
        }
    }

    // 2. 关闭并删除表文件
    rm_manager_->close_file(fhs_.at(tab_name).get());
    fhs_.erase(tab_name);
    rm_manager_->destroy_file(tab_name);

    // 3. 删除元数据
    db_.tabs_.erase(tab_name);

    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name,
                             const std::vector<std::string>& col_names,
                             Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    // ===== 实验四：申请表级 IX 锁（防御性空指针检查）=====
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        auto fh = fhs_.at(tab_name).get();
        context->lock_mgr_->lock_IX_on_table(context->txn_, fh->GetFd());
    }

    // 检查索引是否已存在
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }

    // 收集索引列的元信息
    std::vector<ColMeta> index_cols;
    for (auto &col_name : col_names) {
        auto col = tab.get_col(col_name);
        index_cols.push_back(*col);
    }

    // 1. 创建索引文件
    ix_manager_->create_index(tab_name, index_cols);

    // 2. 打开索引
    auto ih = ix_manager_->open_index(tab_name, col_names);
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);

    // 3. 扫描表，插入已有记录
    auto fh = fhs_.at(tab_name).get();
    int tot_len = 0;
    for (auto &col : index_cols) {
        tot_len += col.len;
    }
    
    for (auto scan = std::make_unique<RmScan>(fh); !scan->is_end(); scan->next()) {
        auto rec = fh->get_record(scan->rid(), context);
        char *key = new char[tot_len];
        int offset = 0;
        for (auto &col : index_cols) {
            memcpy(key + offset, rec->data + col.offset, col.len);
            offset += col.len;
        }
        ih->insert_entry(key, scan->rid(), context ? context->txn_ : nullptr);
        delete[] key;
    }

    // 4. 保存索引句柄
    ihs_.emplace(index_name, std::move(ih));

    // 5. 更新元数据
    IndexMeta index_meta;
    index_meta.tab_name = tab_name;
    index_meta.col_num = col_names.size();
    index_meta.col_tot_len = tot_len;
    index_meta.cols = index_cols;
    tab.indexes.push_back(index_meta);
    
    // 更新列的 index 标记（单列索引情况）
    if (col_names.size() == 1) {
        auto col = tab.get_col(col_names[0]);
        col->index = true;
    }
    
    flush_meta();
}


/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name,
                           const std::vector<std::string>& col_names,
                           Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    // ===== 实验四：申请表级 IX 锁（防御性空指针检查）=====
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        auto fh = fhs_.at(tab_name).get();
        context->lock_mgr_->lock_IX_on_table(context->txn_, fh->GetFd());
    }

    // 检查索引是否存在
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 1. 关闭并删除索引
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    ihs_.erase(index_name);
    ix_manager_->destroy_index(tab_name, col_names);

    // 2. 更新元数据 - 从 indexes 中移除
    auto it = tab.indexes.begin();
    while (it != tab.indexes.end()) {
        if (it->col_num == (int)col_names.size()) {
            bool match = true;
            for (size_t i = 0; i < col_names.size(); i++) {
                if (it->cols[i].name != col_names[i]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                it = tab.indexes.erase(it);
                break;
            }
        }
        ++it;
    }

    // 更新列的 index 标记（单列索引情况）
    if (col_names.size() == 1) {
        auto col = tab.get_col(col_names[0]);
        col->index = false;
    }

    flush_meta();
}