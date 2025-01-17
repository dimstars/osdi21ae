#pragma once 

#include <cstring>
#include <string>
#include <libpmem.h>
#include <sys/time.h>
#include <assert.h>
#include <atomic>
#include <unistd.h>

#include "nvm_allocator.h"
#include "statistic.h"
#include "debug.h"

const int NVM_NodeSize = 256;
const int NVM_KeySize = 8;
const int NVM_ValueSize = 256;
const int NVM_PointSize = 8;
const int NVM_KeyBuf = NVM_KeySize + NVM_PointSize;
const int EntryInterval = 128;
const double Double1 = 1.0;
const uint64_t MAX_KEY = ~(0ULL);
// Statistic stats;

const uint64_t PutOps = 400000000;
const uint64_t GetOps = 1000000;
const uint64_t DeleteOps = 1000000;
const uint64_t ScanOps = 100000;
const uint64_t ScanCount = 1000;
const uint64_t MAX_DRAM_BTREE_SIZE = 1000000000;

static inline int KeyCompare(const void *key1, const void *key2) {
    return memcmp(key1, key2, NVM_KeySize);
}

static inline uint64_t char8toint64(const char *key) {
    uint64_t value = ((((uint64_t)key[0]) & 0xff) << 56) | ((((uint64_t)key[1]) & 0xff) << 48) |
            ((((uint64_t)key[2]) & 0xff) << 40) | ((((uint64_t)key[3]) & 0xff) << 32) |
            ((((uint64_t)key[4]) & 0xff) << 24) | ((((uint64_t)key[5]) & 0xff) << 16) |
            ((((uint64_t)key[6]) & 0xff) << 8)  | (((uint64_t)key[7]) & 0xff);
    return value;

}

extern NVMAllocator *node_alloc[];
extern NVMAllocator *value_alloc[];
extern NVMLogPool *log_alloc_pool[];

static inline void fillchar8wirhint64(char *key, uint64_t value) {
    key[0] = ((char)(value >> 56)) & 0xff;
    key[1] = ((char)(value >> 48)) & 0xff;
    key[2] = ((char)(value >> 40) )& 0xff;
    key[3] = ((char)(value >> 32)) & 0xff;
    key[4] = ((char)(value >> 24)) & 0xff;
    key[5] = ((char)(value >> 16)) & 0xff;
    key[6] = ((char)(value >> 8)) & 0xff;
    key[7] = ((char)value) & 0xff;
}

static inline double CaculateCDF(const char *key, uint64_t min_key, uint64_t max_key) {
    uint64_t tmp = char8toint64(key);
    if(tmp < min_key) {
        return -Double1;
    } else if(tmp > max_key) {
        return Double1 * 2;
    }
    double CDFI = (Double1 * (tmp - min_key)) / (max_key - min_key);
    return CDFI ;
}

static inline uint64_t get_now_micros(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
}

extern atomic<uint64_t> perist_data;

static inline void show_persist_data() {
    print_log(LV_INFO, "Persit data is %ld %lf GB.", perist_data.load(std::memory_order_relaxed), (1.0 * perist_data) / 1000 / 1000/ 1000);
}

static inline void nvm_persist(const void *addr, size_t len) {
    perist_data += len;
    // print_log(LV_DEBUG, "perist_data is %ld, len is %ld", perist_data.load(std::memory_order_relaxed), len);
    // show_persist_data();
    pmem_persist(addr, len);
}

static inline void nvm_memcpy_persist(void *pmemdest, const void *src, size_t len, bool statistic = true) {
    if(statistic) {
        perist_data += len;
    }
    pmem_memcpy_persist(pmemdest, src, len);
}