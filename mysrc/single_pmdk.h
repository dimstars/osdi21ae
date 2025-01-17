/*
   Copyright (c) 2018, UNIST. All rights reserved. The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST.

   Please use at your own risk.
*/
#pragma once 

#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <queue>
#include <thread>

#include "nvm_common.h"

#define PAGESIZE 512

#define CACHE_LINE_SIZE 64

#define IS_FORWARD(c) (c % 2 == 0)

#define IS_VALID_PTR(p) (((uint64_t)p & 0x700000000000) == 0x700000000000) 

class nvmpage;
class subtree;
class btree;
class bpnode;
class MyBtree;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_ROOT(btree, MyBtree);
POBJ_LAYOUT_TOID(btree, nvmpage);
//POBJ_LAYOUT_TOID(btree, subtree);
POBJ_LAYOUT_END(btree);

using entry_key_t = uint64_t;

using namespace std;

class nvmheader {
private:
  nvmpage *leftmost_ptr;     // 8 bytes
  nvmpage *sibling_ptr; // 8 bytes
  std::mutex *mtx;
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  char * none;          //8 bytes

  friend class nvmpage;
  friend class bpnode;
  friend class subtree;
  friend class btree;

public:
  void constructor() {
    leftmost_ptr = NULL;
    //TOID_ASSIGN(sibling_ptr, pmemobj_oid(this));
    //sibling_ptr.oid.off = 0;
    sibling_ptr = nullptr;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  nvmheader() {
    mtx = new std::mutex();
  }

  ~nvmheader() {
    delete mtx;
  }
};

class nvmentry {
private:
  entry_key_t key; // 8 bytes
  char *ptr;       // 8 bytes

public:
  void constructor() {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class nvmpage;
  friend class bpnode;
  friend class btree;
  friend class subtree;
};

const int nvm_cardinality = (PAGESIZE - sizeof(nvmheader)) / sizeof(nvmentry);
const int nvm_count_in_line = CACHE_LINE_SIZE / sizeof(nvmentry);

// 子树在NVM中的节点
class nvmpage {
private:
  nvmheader hdr;                 // nvmheader in persistent memory, 32 bytes
  nvmentry records[nvm_cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree;
  friend class subtree;
  friend class bpnode;

  inline void lock() {
    hdr.mtx->lock();
  }

  inline void unlock() {
    hdr.mtx->unlock();
  }

  void constructor(uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < nvm_cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  void constructor(PMEMobjpool *pop, nvmpage *left, entry_key_t key, nvmpage *right,
                    uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < nvm_cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    pmemobj_persist(pop, this, sizeof(nvmpage));
  }

  inline int count() {
    uint8_t previous_switch_counter;
    int count = 0;

    do {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL) {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0) {
        count = 0;
        while (records[count].ptr != NULL) {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(PMEMobjpool *pop, entry_key_t key) {
    lock();
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;

        // flush
        uint64_t records_ptr = (uint64_t)(&records[i]);
        int remainder = records_ptr % CACHE_LINE_SIZE;
        bool do_flush =
            (remainder == 0) ||
            ((((int)(remainder + sizeof(nvmentry)) / CACHE_LINE_SIZE) == 1) &&
              ((remainder + sizeof(nvmentry)) % CACHE_LINE_SIZE) != 0);
        if (do_flush) {
          pmemobj_persist(pop, (void *)records_ptr, CACHE_LINE_SIZE);
        }
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    unlock();
    return shift;
  }


  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false,
            bool with_lock = true, subtree* sub_root = NULL);
  bool merge(btree *bt, bpnode *left_sibling, entry_key_t deleted_key_from_parent, subtree* sub_root, subtree* left_subtree_sibling);

  nvmpage *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
            subtree *sub_root = NULL, nvmpage *invalid_sibling = NULL);

  void insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                      int *num_entries, bool flush = true,
                      bool update_last_index = true);
  bool update_key(PMEMobjpool *pop, entry_key_t key, char *ptr);


  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size, uint64_t base = 0);
  void linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size, uint64_t base);
  void linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::string>& results, int &size, uint64_t base);

  void linear_search_range(entry_key_t min, entry_key_t max,
                            unsigned long *buf, uint64_t base = 0) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    nvmpage *current = this;

    while (current) {
      int old_off = off;
      do {
        previous_switch_counter = current->hdr.switch_counter;
        off = old_off;

        entry_key_t tmp_key;
        char *tmp_ptr;

        if (IS_FORWARD(previous_switch_counter)) {
          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              return;
            }
          }

          for (i = 1; current->records[i].ptr != NULL; ++i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                return;
              }
            }
          }
        } else {
          for (i = current->count() - 1; i > 0; --i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                return;
              }
            }
          }

          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              return;
            }
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      // todo
      if (IS_VALID_PTR(current->hdr.sibling_ptr) || base == 0) {
        current = current->hdr.sibling_ptr;
      } else {
        current = (nvmpage *)((uint64_t)current->hdr.sibling_ptr + base);
      }
    }
  }

  char *linear_search(entry_key_t key) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }
      /*
      if ((t = (char *)hdr.sibling_ptr.oid.off) &&
          key >= D_RW(hdr.sibling_ptr)->records[0].key) {
        return t;
      }
      */
      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; i <= hdr.last_index && records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                //printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            //printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);
      /*
      if ((t = (char *)hdr.sibling_ptr.oid.off) != NULL) {
        if (key >= D_RW(hdr.sibling_ptr)->records[0].key)
          return t;
      }*/

      if (ret) {
        //printf("key: %ld  ret: %x", key, ret);
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }

  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %x \n", this->hdr.level, (uint64_t)pmemobj_oid(this).off);
    else
      printf("[%d] internal %x \n", this->hdr.level, pmemobj_oid(this).off);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%x ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%x ", records[i].key, records[i].ptr);

    printf("%x ", (uint64_t)hdr.sibling_ptr);

    printf("\n");
  }

  void printAll() {
    TOID(nvmpage) p = TOID_NULL(nvmpage);
    TOID_ASSIGN(p, pmemobj_oid(this));

    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      p.oid.off = (uint64_t)hdr.leftmost_ptr;
      D_RW(p)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        p.oid.off = (uint64_t)records[i].ptr;
        D_RW(p)->printAll();
      }
    }
  }
};

class RebalanceTask {
  public:
    subtree * left;
    subtree * right;
    bpnode * cur_d; 
    nvmpage * cur_n;
    entry_key_t deleted_key_from_parent;

    RebalanceTask(subtree * left, subtree * right, bpnode * cur_d, nvmpage * cur_n, entry_key_t deleted_key_from_parent) {
      this->left = left;
      this->right = right;
      this->cur_d = cur_d; 
      this->cur_n = cur_n;
      this->deleted_key_from_parent = deleted_key_from_parent;
    }
};

class MyBtree{
  private:
    PMEMobjpool *pop[NUMACOUNT];
    subtree * head;  // off
    int time_;       // sec
    int subtree_num; // dram subtree num
    btree * bt;
    bool switch_;
    static MyBtree * mybt;
    std::thread heat_thread;
    std::thread migrate_thread;
  public:
    static MyBtree *getInitial(string persistent_path = "") {
      if (mybt == nullptr) {
        TOID(MyBtree) nvmbt = TOID_NULL(MyBtree);
        PMEMobjpool *pop[NUMACOUNT];
        if (file_exists_((ROOTPATH + std::to_string(0) + persistent_path).c_str()) != 0) {
          for (int i = 0; i < NUMACOUNT; ++i) {
            pop[i] = pmemobj_create((ROOTPATH + std::to_string(i) + persistent_path).c_str(), "btree", 30000000000,
                                0666); // make 30GB memory pool
            printf("create pmemobjpool: %p\n\n",pop[i]);
          }
          // pop[0] = pmemobj_create((ROOTPATH + std::to_string(0) + persistent_path).c_str(), "btree", 30000000000,
          //                        0666); // make 30GB memory pool
          // printf("create pmemobjpool: %p\n\n",pop[0]);
          // pop[1] = pop[0];
          // printf("create pmemobjpool: %p\n\n",pop[1]);
          nvmbt = POBJ_ROOT(pop[0], MyBtree);
          D_RW(nvmbt)->constructor(pop);
        } else {
          for (int i = 0; i < NUMACOUNT; ++i) {
            pop[i] = pmemobj_open((ROOTPATH + std::to_string(i) + persistent_path).c_str(), "btree");
            printf("open pmemobjpool: %p\n\n",pop[i]);
          }
          nvmbt = POBJ_ROOT(pop[0], MyBtree);
          D_RW(nvmbt)->Recover(pop);
        }
        mybt = D_RW(nvmbt);

        // 开启后台热度统计线程
        /*heat_thread.emplace_back(
        [&](size_t thread_id) {
          printf("Heat thread is opened\n");
          while (!stop) {
            bt.check
            sleep(60 * 10);
          }     
        }*/
      }
      return mybt;
    }

    btree * getBt() {
      return bt;
    }

    void setHead(subtree * head) {
      this->head = head;
      pmemobj_persist(pop[my_thread_id % NUMACOUNT], &(this->head), sizeof(this->head));
    }

    inline subtree *to_nvmptr(subtree *off) {
      if (off == nullptr) return nullptr;
      return (subtree *)((uint64_t)off + (uint64_t)pop[my_thread_id % NUMACOUNT]);
    }

    void constructor(PMEMobjpool * pool[]);
    void Recover(PMEMobjpool *pool[]);
    void Redistribute();
    void later();
    void exitBtree();
    void clearHeat();
    void closeChange() {
      switch_ = false;
    }
    void setSubtreeNum(int num) {
      this->subtree_num = num;
      pmemobj_persist(pop[0], &(this->subtree_num), sizeof(subtree_num));
      // pmemobj_persist(pop[my_thread_id % NUMACOUNT], &(this->subtree_num), sizeof(subtree_num));
    }

    PMEMobjpool ** getPoolPtr() {
      return this->pop;
    }
};

class subtree {
  private:
    bpnode* dram_ptr;
    nvmpage* nvm_ptr;     // off 
    subtree* sibling_ptr; // off
    subtree* pre_ptr;     // off
    subtree* tmp_ptr;     // off
    uint64_t heat;        // 总热度
    uint64_t numa_heat[NUMACOUNT];  // 每个节点的热度
    uint64_t numa_visit[NUMACOUNT]; // 每个节点一段时间内的访问次数
    uint64_t kv_nums;
    int32_t hotspot_numa; // 热点所在numa节点
    int32_t nvm_numa;     // 子树在NVM中的部分所在的numa节点
    PMEMobjpool *pop[NUMACOUNT];
    uint64_t log_off;  // off
    LogAllocator* log_alloc;
    RebalanceTask *rt;
    bool flag; // true:dram   false:nvm
    bool change; // try to cache this subtree
    bool lock;
    bool transing;
    std::mutex *mtx;

    inline void subtree_lock() {
      //printf("subtree lock\n");
      mtx->lock();
    }

    inline void subtree_unlock() {
      //printf("subtree unlock\n");
      mtx->unlock();
    }

  public:
    void constructor(PMEMobjpool *pop[], int32_t numa_node, bpnode* dram_ptr, subtree* pre = nullptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = true) {
      this->flag = flag;
      this->dram_ptr = dram_ptr;
      this->nvm_ptr = nullptr;
      this->heat = heat;
      this->kv_nums = -1;
      for (int i = 0; i < NUMACOUNT; ++i) {
        this->pop[i] = pop[i];
      }
      this->nvm_numa = numa_node;
      this->log_off = getNewLogAllocator(nvm_numa);
      this->log_alloc = node_alloc[numa_node]->getNVMptr(log_off);
      this->rt = nullptr;
      this->change = flag;
      this->lock = false;
      this->transing = false;

      if (next != nullptr) {
        //this->sibling_ptr = (subtree *)pmemobj_oid(next).off;
        this->sibling_ptr = next;
      } else {
        this->sibling_ptr = nullptr;
      }

      if (pre != nullptr) {
        //this->pre_ptr = (subtree *)pmemobj_oid(pre).off;
        this->pre_ptr = pre;
      } else {
        this->pre_ptr = nullptr;
      }
      //pmemobj_persist(pop, this, sizeof(subtree));
    }

    void constructor(PMEMobjpool *pop[], int32_t numa_node, nvmpage* nvm_ptr, subtree* pre = nullptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = false) {
      this->flag = flag;
      this->dram_ptr = nullptr;
      this->nvm_ptr = nvm_ptr;
      this->heat = heat;
      this->kv_nums = -1;
      for (int i = 0; i < NUMACOUNT; ++i) {
        this->pop[i] = pop[i];
      }
      this->nvm_numa = numa_node;
      this->log_off = -1;
      this->log_alloc = nullptr;
      this->rt = nullptr;
      this->change = flag;
      this->lock = false;
      this->transing = false;

      if (next != nullptr) {
        //this->sibling_ptr = (subtree *)pmemobj_oid(next).off;
        this->sibling_ptr = next;
      } else {
        this->sibling_ptr = nullptr;
      }

      if (pre != nullptr) {
        //this->pre_ptr = (subtree *)pmemobj_oid(pre).off;
        this->pre_ptr = pre;
      } else {
        this->pre_ptr = nullptr;
      }

      //pmemobj_persist(pop, this, sizeof(subtree));
    }

    subtree() {
      mtx = new std::mutex();
    }

    ~subtree() {
      delete mtx;
    }

    void subtree_insert(btree* root, entry_key_t key, char* right, bool wal = true);
    void subtree_update(btree* root, entry_key_t key, char* right, bool wal = true);
    void subtree_delete(btree* root, entry_key_t, bool wal = true);
    char *subtree_search(entry_key_t);
    void subtree_search_range(entry_key_t, entry_key_t, unsigned long *); 
    void subtree_search_range(entry_key_t, entry_key_t, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size); 
    void subtree_search_range(entry_key_t, entry_key_t, void **values, int &size); 
    void subtree_search_range(entry_key_t, entry_key_t, std::vector<std::string> &values, int &size); 

    void btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level, btree* bt);
    void btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
        bool *is_leftmost_node, bpnode **left_sibling, btree* bt);
    void btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
        bool *is_leftmost_node, nvmpage **left_sibling, btree* bt);

    // nvm --> dram
    char* DFS(nvmpage* root, bpnode **pre);
    void nvm_to_dram(bpnode **pre);
    void dram_recovery(bpnode **pre);
    void leaf_to_dram();

    // dram --> nvm
    char* DFS(char* root, nvmpage **pre, bool ifdel = true);
    void dram_to_nvm(nvmpage **pre);

    // sync dram --> nvm
    void sync_subtree(nvmpage **pre);

    inline nvmpage *to_nvmpage(nvmpage *off) {
      if (off == nullptr)  return nullptr;
      return (nvmpage *)((uint64_t)off + (uint64_t)pop[nvm_numa]);
    }

    inline nvmpage *to_nvmpage(char *off) {
      if (off == nullptr)  return nullptr;
      return (nvmpage *)((uint64_t)off + (uint64_t)pop[nvm_numa]);
    }

    inline nvmpage *get_nvmroot_ptr() {
      return to_nvmpage(nvm_ptr);
    }

//#undef ENABLE_NFTREE
    uint64_t getHeat() {
      return heat;
    }

    void setHeat(uint64_t heat) {
      this->heat = heat;
      //pmemobj_persist(pop, &this->heat, sizeof(uint64_t));
    }

    void increaseHeat() {
      numa_visit[my_thread_id % NUMACOUNT] += 1;
    }

    void refreshHeat() {
      uint64_t new_heat = 0;
      for (int i = 0; i < NUMACOUNT; i++) {
        numa_heat[i] = (numa_heat[i] + numa_visit[i]) / 2;
        numa_visit[i] = 0;
        new_heat += numa_heat[i];
      }
      heat = new_heat;
      hotspot_numa = -1;
      for (int i = 0; i < NUMACOUNT; i++) {
        if (numa_heat[i] >= (heat / 5) * 3) {
          hotspot_numa = i;
          break;
        }
      }
    }

    subtree * getSiblingPtr() {
      if (sibling_ptr == nullptr) return nullptr;
      //return (subtree *)((uint64_t)sibling_ptr + (uint64_t)pop);
      return sibling_ptr;
    }

    subtree * getPrePtr() {
      if (pre_ptr == nullptr) return nullptr;
      //return (subtree *)((uint64_t)pre_ptr + (uint64_t)pop);
      return pre_ptr;
    }

    void setSiblingPtr(subtree *ptr) {
      sibling_ptr = ptr;
      //pmemobj_persist(pop, &sibling_ptr, sizeof(subtree *));
    }

    void setPrePtr(subtree *ptr) {
      pre_ptr = ptr;
      //pmemobj_persist(pop, &pre_ptr, sizeof(subtree *));
    }

    void setNewDramRoot(bpnode *ptr) {
      dram_ptr = ptr;
      //pmemobj_persist(pop, &dram_ptr, sizeof(bpnode *));
    }

    void setNewNvmRoot(nvmpage *ptr) {
      nvm_ptr = ptr;
      //pmemobj_persist(pop, &nvm_ptr, sizeof(nvmpage *));
    }

    bool isNVMBtree() {
      return !flag;
    }

    bool rebalance(btree * bt);

    bool needRebalance() {
      if (rt != nullptr) {
        return true;
      }
      return false;
    }

    void deleteRt() {
      delete rt;
      rt = nullptr;
    }

    bool getState() {
      return change;
    }

    void flushState() {
      change = false;
    }

    void PrintInfo() {
      printf("subtree: %p is dram: %d  heat: %lu\n", this, flag, heat);
      if(log_alloc) { 
        log_alloc->PrintStorage();
      }
    }

    bpnode *getLastDDataNode();
    nvmpage *getLastNDataNode();
    bpnode *getFirstDDataNode();
    nvmpage *getFirstNDataNode();
    void *getLastLeafNode();
    void *getFirstLeafNode();
    bpnode *getDramDataNode(char *ptr);
    nvmpage *getNvmDataNode(char *ptr);
    entry_key_t getFirstKey();
    void recover();
    void recovery(btree* bt);

    friend class bpnode;
    friend class nvmpage;
    friend class MyBtree;
    friend class btree;
};

static subtree* newSubtreeRoot(PMEMobjpool *pop[], int32_t numa_node, bpnode *subtree_root, subtree * pre = nullptr) {
    // TOID(subtree) node = TOID_NULL(subtree);
    // POBJ_NEW(pop, &node, subtree, NULL, NULL);
    // if (pre) {
    //   // 分裂生成
    //   D_RW(node)->constructor(pop, subtree_root, pre, pre->getSiblingPtr(), pre->getHeat() * 2 / 3);
    // } else {
    //   // 新生成
    //   D_RW(node)->constructor(pop, subtree_root);
    // }
    // return D_RW(node);
    subtree *node = new subtree;
    if (pre) {
      node->constructor(pop, numa_node, subtree_root, pre, pre->getSiblingPtr(), pre->getHeat() * 2 / 3);
    } else {
      node->constructor(pop, numa_node, subtree_root);
    }
    return node;
}

static subtree* newSubtreeRoot(PMEMobjpool *pop[], int32_t numa_node, nvmpage *subtree_root, subtree * pre = nullptr) {
    // TOID(subtree) node = TOID_NULL(subtree);
    // POBJ_NEW(pop, &node, subtree, NULL, NULL);
    // if (pre) {
    //   // 分裂生成
    //   D_RW(node)->constructor(pop, subtree_root, pre, pre->getSiblingPtr(), pre->getHeat() / 2);
    // } else {
    //   // 新生成
    //   D_RW(node)->constructor(pop, subtree_root);
    // }
    // return D_RW(node);
    subtree *node = new subtree;
    if (pre) {
      node->constructor(pop, numa_node, subtree_root, pre, pre->getSiblingPtr(), pre->getHeat() / 2);
    } else {
      node->constructor(pop, numa_node, subtree_root);
    }
    return node;
}

struct cmp {
    bool operator()(subtree* a, subtree* b) {
        return a->getHeat() > b->getHeat();
    }
};