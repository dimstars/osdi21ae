#include "nvm_common.h"
#include "nvm_allocator.h"

NVMAllocator *node_alloc[NUMACOUNT] = {0};
NVMAllocator *value_alloc[NUMACOUNT] = {0};
NVMLogPool *log_alloc_pool[NUMACOUNT] = {0};

atomic<uint64_t> perist_data(0);

int AllocatorInit(const std::string &logpath, uint64_t logsize, const std::string &allocator_path, 
                uint64_t allocator_size) {
    for (int i = 0; i < NUMACOUNT; ++i) {
        log_alloc_pool[i] = new NVMLogPool(ROOTPATH + std::to_string(i) + logpath, logsize);
        if(log_alloc_pool[i] == nullptr) {
            delete log_alloc_pool[i];
            return -1;
        }
        node_alloc[i] = new NVMAllocator(ROOTPATH + std::to_string(i) + allocator_path, allocator_size);
        if(node_alloc[i] == nullptr) {
            delete node_alloc[i];
            return -1;
        }
    }
    // perist_data = 0;
    return 0;
}

int AllocatorInit(const std::string &logpath, uint64_t logsize, const std::string &valuepath, uint64_t valuesize, const std::string &allocator_path, 
                uint64_t allocator_size) {
    for (int i = 0; i < NUMACOUNT; ++i) {
        log_alloc_pool[i] = new NVMLogPool(ROOTPATH + std::to_string(i) + logpath, logsize);
        if(log_alloc_pool[i] == nullptr) {
            delete log_alloc_pool[i];
            return -1;
        }
        value_alloc[i] = new NVMAllocator(ROOTPATH + std::to_string(i) + valuepath, valuesize);
        if(value_alloc[i] == nullptr) {
            delete value_alloc[i];
            return -1;
        }
        node_alloc[i] = new NVMAllocator(ROOTPATH + std::to_string(i) + allocator_path, allocator_size);
        if(node_alloc[i] == nullptr) {
            delete node_alloc[i];
            return -1;
        }
    }
    // perist_data = 0;
    return 0;
}

void AllocatorExit() {
    for (int i = 0; i < NUMACOUNT; ++i) {
        if(node_alloc[i]) {
            delete node_alloc[i];
        } 

        if(value_alloc[i]) {
            delete value_alloc[i];
        }

        if(log_alloc_pool[i]) {
            delete log_alloc_pool[i];
        }
    }
}

void LogAllocator::writeKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(1, off, key, (uint64_t)value);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::updateKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(2, off, key, (uint64_t)value);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::deleteKey(uint64_t off, int64_t key) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(0, off, key, 0);
    nvm_memcpy_persist(logvalue, &tmp, 24);
}

void LogAllocator::operateTree(uint64_t src, uint64_t dst, int64_t key, int64_t type) {
    // 3分裂  子树内 
    // 4分裂  子树间   分裂后下刷前出问题恢复
    // 5合并  子树内 dram -- dram
    // 6合并  子树间 dram --> dram  合并后下刷前出问题恢复
    // 7合并  子树间 dram <-- dram
    // 8合并  子树间 dram <-- dram 完全合并
    // 9合并 子树间 nvm <-- dram 转换成update    dram <-- nvm 转换成insert
    // 10合并 子树间 dram --> nvm 转换成update    nvm --> dram 转换成insert
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(type, src, dst, key);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::log_persist(char* addr, uint64_t aligns) {
    if ((uint64_t)addr + 16 - (uint64_t)last_index_ > aligns) {
        nvm_persist(last_index_, aligns);
        last_index_ = addr;
    } 
}

void LogAllocator::writeKv(int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(16);
    SimpleLogNode tmp(1, key, (uint64_t)value);
    memcpy(logvalue, &tmp, 16);
    //log_persist(logvalue + 16);
    nvm_persist(logvalue, 16);
}

void LogAllocator::updateKv(int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(16);
    SimpleLogNode tmp(2, key, (uint64_t)value);
    memcpy(logvalue, &tmp, 16);
    //log_persist(logvalue + 16);
    nvm_persist(logvalue, 16);
}

void LogAllocator::deleteKey(int64_t key) {
    char* logvalue = this->AllocateAligned(16);
    SimpleLogNode tmp(0, key, 0);
    memcpy(logvalue, &tmp, 16);
    //log_persist(logvalue + 16);
    nvm_persist(logvalue, 16);
}

void LogAllocator::operateTree(int64_t key, int64_t type) {
    // nvm --> dram   dram下刷，下刷时失败回滚即可
    // nvm <-- dram   写log 恢复dram的时候修改根节点 3
    // dram --> nvm   写log 恢复dram的时候将records[m].ptr = nullptr即可 4
    // dram <-- nvm   dram下刷，下刷时失败回滚即可
    char* logvalue = this->AllocateAligned(16);
    SimpleLogNode tmp(type, key, 0);
    memcpy(logvalue, &tmp, 16);
    //log_persist(logvalue + 16);
    nvm_persist(logvalue, 16);
}

static void alloc_memalign(void **ret, size_t alignment, size_t size) {
    // posix_memalign(ret, alignment, size);
    char *mem =  node_alloc[my_thread_id % NUMACOUNT]->Allocate(size);
    *ret = mem;
}

void *LogAllocator::operator new(size_t size) {
    void *ret;
    alloc_memalign(&ret, 64, size);
    return ret;
}