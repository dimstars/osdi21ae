#include "../bench/bench.h"
#include "single_pmdk.h"
#include "single_btree.h"

#ifdef ENABLE_NAP
#undef ENABLE_NAP
#endif

#define LOGPATH "/log_persistent"
#define PATH "/ycsb"
#define NODEPATH "/persistent"
#define VALUEPATH "/data"

const uint64_t NVM_NODE_SIZE = 20 * (1ULL << 30);
const uint64_t NVM_LOG_SIZE = 30 * (1ULL << 30);
const uint64_t NVM_VALUE_SIZE = 30 * (1ULL << 30);

char max_str[15] = {(int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255,
                    (int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255,
                    (int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255};

namespace {

class NFTree {
public:
    NFTree(): tree_(nullptr) {
      AllocatorInit(LOGPATH, NVM_LOG_SIZE, VALUEPATH, NVM_VALUE_SIZE, NODEPATH, NVM_NODE_SIZE);
    }

    NFTree(btree *tree): tree_(tree) {}
    virtual ~NFTree() {
      mybt->exitBtree();
      AllocatorExit();
    }

    void Init()
    {
      mybt = MyBtree::getInitial(PATH);
      tree_ = mybt->getBt();
    }

    void Info()
    {
      mybt->clearHeat();
    }

    void Close() { 
      mybt->closeChange();
    }
    int Put(uint64_t key, uint64_t value) 
    {
        tree_->btreeInsert(key, (char *)value);
        return 1;
    }
    int Get(uint64_t key, uint64_t &value)
    {
        value = (uint64_t)tree_->btreeSearch(key);
        return 1;
    }
    int Update(uint64_t key, uint64_t value) {
        //tree_->btreeDelete(key);
        tree_->btreeUpdate(key, (char *)value);
        return 1;
    }
    int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
    {
        tree_->btreeSearchRange(start_key, UINT64_MAX, results, len);
        return 1;
    }

    btree *get_btree() {
      return tree_;
    }

private:
    btree *tree_;
    MyBtree *mybt;
};

uint64_t sting_to_uint64(const nap::Slice &key) {
  uint64_t k = 0;
  for (int i = 8; i >= 0 ; --i) {
    k = k * 100 + key.data()[i] % 100;
  }
  return k;
}

uint64_t sting_to_uint64(uint8_t key[]) {
  uint64_t k = 0;
  for (int i = 8; i >= 0 ; --i) {
    k = k * 256 + key[i];
  }
  return k;
}

struct NFTreeIndex {
  btree *map;

  NFTreeIndex(btree *map) : map(map) {}

  void put(const nap::Slice &key, const nap::Slice &value, bool is_update) {
    uint64_t k = sting_to_uint64(key);
    map->btreeInsert(k, (char *)(cur_value++));
  }

  bool get(const nap::Slice &key, std::string &value) {
    uint64_t k = sting_to_uint64(key);
    uint64_t *ret =
        reinterpret_cast<uint64_t *>(map->btreeSearch(k));
    return ret != nullptr;
  }

  void del(const nap::Slice &key) {}
};

enum class cceh_op { UNKNOWN, INSERT, READ, MAX_OP };

struct thread_queue {
  uint8_t key[KEY_LEN];
  cceh_op operation;

  thread_queue() { key[KEY_LEN - 1] = 0; }
};

struct alignas(64) sub_thread {
  uint32_t id;
  uint64_t inserted;
  uint64_t found;
  uint64_t unfound;
  uint64_t thread_num;
  thread_queue *run_queue;
  double *latency_queue;
};

} // namespace

int main(int argc, char *argv[]) {

  // parse inputs
  if (argc != 5) {
    printf("usage: %s <pool_path> <load_file> <run_file> <thread_num>\n\n",
           argv[0]);
    printf("    pool_path: the pool file required for PMDK\n");
    printf("    load_file: a workload file for the load phase\n");
    printf("    run_file: a workload file for the run phase\n");
    printf("    thread_num: the number of threads\n");
    exit(1);
  }

  const char *path = argv[1];
  (void)path;
  size_t thread_num;

  std::stringstream s;
  s << argv[4];
  s >> thread_num;

  assert(thread_num > 0);

  init_numa_pool();

  NFTree *nftree = new NFTree();
  nftree->Init();
  btree *tree = nftree->get_btree();

  printf("initialization done.\n");

  // load benchmark files
  FILE *ycsb, *ycsb_read;
  char buf[1024];
  char *pbuf = buf;
  size_t len = 1024;
  uint8_t key[KEY_LEN];
  key[KEY_LEN - 1] = '\0';
  size_t loaded = 0, inserted = 0, found = 0, unfound = 0;

  if ((ycsb = fopen(argv[2], "r")) == nullptr) {
    printf("failed to read %s\n", argv[2]);
    exit(1);
  }

  printf("Load phase begins \n");
  while (getline(&pbuf, &len, ycsb) != -1) {
    if (strncmp(buf, "INSERT", 6) == 0) {
      memcpy(key, buf + 7, KEY_LEN - 1);

      //   printf("%d\n", strlen((char *)key));
      // assert(strlen((char *)key) == 14);

      uint64_t k = sting_to_uint64(key);
      tree->btreeInsert(k, (char *)(cur_value++));
      loaded++;

      next_thread_id_for_load(thread_num);
    }
  }

  fclose(ycsb);
  printf("Load phase finishes: %ld items are inserted \n", loaded);

  if ((ycsb_read = fopen(argv[3], "r")) == nullptr) {
    printf("fail to read %s\n", argv[3]);
    exit(1);
  }

  thread_queue *run_queue[thread_num];
  double *latency_queue[thread_num];
  int move[thread_num];
  for (size_t t = 0; t < thread_num; t++) {
    run_queue[t] = new thread_queue[READ_WRITE_NUM / thread_num + 1];
    latency_queue[t] =
        (double *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(double));
    move[t] = 0;
  }

  size_t operation_num = 0;
  while (getline(&pbuf, &len, ycsb_read) != -1) {
    auto cur = operation_num % thread_num;
    if ((size_t)move[cur] >= READ_WRITE_NUM / thread_num + 1) {
      break;
    }

    auto &e = run_queue[cur][move[cur]];

    if (strncmp(buf, "INSERT", 6) == 0 || strncmp(buf, "UPDATE", 6) == 0) {

      memcpy(e.key, buf + 7, KEY_LEN - 1);
      e.key[KEY_LEN - 1] = '\0';

      // printf("%d\n", strlen((char *)e.key));
      // assert(strlen((char *)e.key) == 14);
      e.operation = cceh_op::INSERT;
      move[cur]++;
    } else if (strncmp(buf, "READ", 4) == 0) {
      memcpy(e.key, buf + 5, KEY_LEN - 1);
      e.key[KEY_LEN - 1] = '\0';

      // printf("%d\n", strlen((char *)e.key));
      // assert(strlen((char *)e.key) == 14);
      e.operation = cceh_op::READ;
      move[cur]++;
    } else {
      assert(false);
    }
    operation_num++;
  }
  fclose(ycsb_read);

  sub_thread *THREADS = (sub_thread *)malloc(sizeof(sub_thread) * thread_num);
  inserted = 0;

  printf("Run phase begins: %s \n", argv[3]);
  for (size_t t = 0; t < thread_num; t++) {
    THREADS[t].id = t;
    THREADS[t].inserted = 0;
    THREADS[t].found = 0;
    THREADS[t].unfound = 0;
    THREADS[t].thread_num = thread_num;
    THREADS[t].run_queue = run_queue[t];
    THREADS[t].latency_queue = latency_queue[t];
  }

#ifdef LATENCY_ENABLE
  struct timespec stop;
#endif

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

#ifdef ENABLE_NAP
  NFTreeIndex raw_index(tree);
  nap::Nap<NFTreeIndex> nftree_nap(&raw_index);
#endif

  // warm up
  {
    const std::string warm_up("/home/ljr/Nap/dataset/load4");
    if ((ycsb = fopen(warm_up.c_str(), "r")) == nullptr) {
      printf("failed to read %s\n", warm_up.c_str());
      exit(1);
    }
    printf("Warmup phase begins \n");
    while (getline(&pbuf, &len, ycsb) != -1) {
      if (strncmp(buf, "READ", 4) == 0) {
        memcpy(key, buf + 5, KEY_LEN - 1);

#ifdef ENABLE_NAP
        std::string str;
        nftree_nap.get(nap::Slice((char *)key, KEY_LEN), str);
#endif
      }
    }
#ifdef ENABLE_NAP
    nap::Topology::reset();

    nftree_nap.set_sampling_interval(32);
#endif
    fclose(ycsb);
  }

  constexpr int kTestThread = nap::kMaxThreadCnt;
  struct timespec start[kTestThread], end[kTestThread];
  bool is_test[kTestThread];
  memset(is_test, false, sizeof(is_test));

  std::atomic<uint64_t> th_counter{0};
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [&](size_t thread_id) {
          my_thread_id = nap::Topology::threadID();
          printf("Thread %d is opened\n", my_thread_id);
          bindCore(my_thread_id);

          constexpr int kBenchLoop = 1;
          for (int k = 0; k < kBenchLoop; ++k) {
            if (k == kBenchLoop - 1) { // enter into benchmark
              th_counter.fetch_add(1);
              while (th_counter.load() != thread_num) {
                ;
              }

#ifdef ENABLE_NAP
              nftree_nap.clear();
#endif
              clock_gettime(CLOCK_MONOTONIC, start + my_thread_id);
              is_test[my_thread_id] = true;
            }
            for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++) {

              auto &op = THREADS[thread_id].run_queue[j];
              if (op.operation == cceh_op::INSERT) {

#ifdef ENABLE_NAP
                nftree_nap.put(nap::Slice((char *)op.key, KEY_LEN),
                                 nap::Slice((char *)op.key, 8), false);
#else
                uint64_t k = sting_to_uint64(op.key);
                tree->btreeInsert(k, (char *)(cur_value++));

#endif

              } else if (op.operation == cceh_op::READ) {

#ifdef RANGE_BENCH

#ifdef ENABLE_NAP

                thread_local static char buf_2[4096];
                nftree_nap.internal_query(
                    (nap::Slice((char *)op.key, KEY_LEN)), 10, (char *)buf_2);

                int off = 0;
                tree->btree_search_range((char *)op.key, max_str,
                                         (unsigned long *)thread_local_buffer,
                                         100, off);

            // for (int i = 0; i < 10; ++i) {
            //   strncmp(buf_2 + i * KEY_LEN,
            //   (char *)thread_local_buffer + i * KEY_LEN, KEY_LEN);
            // }

#else
                int off = 0;
                uint64_t k = sting_to_uint64(op.key);
                std::vector<std::pair<uint64_t, uint64_t>> results;
                results.clear();
                nftree->Scan(k, 100, results);
                if (!results.empty() && results.size() >= 99) {
                  THREADS[thread_id].found ++;
                } else {
                  THREADS[thread_id].unfound ++;
                }

#endif
#else

#ifdef ENABLE_NAP
                std::string str;
                if (nftree_nap.get(nap::Slice((char *)op.key, KEY_LEN), str)) {
                  THREADS[thread_id].found ++;
                } else {
                  THREADS[thread_id].unfound ++;
                }

#else
                // printf("XXX %d\n",
                //        strlen((char *)THREADS[thread_id].run_queue[j].key));
                uint64_t k = sting_to_uint64(op.key);
                if (tree->btreeSearch(k)) {
                  THREADS[thread_id].found ++;
                } else {
                  THREADS[thread_id].unfound ++;
                  //printf("not found key: %s , num: %lu\n", op.key, k);
                }
#endif

#endif
              }

              else {
                printf("unknown op\n");
                exit(1);
              }
            }
          }
          clock_gettime(CLOCK_MONOTONIC, end + my_thread_id);

#if defined(RECOVERY_TEST) && defined(ENABLE_NAP)

          if (thread_id == 0) {
            nap::Timer s;
            s.sleep(1000ull * 1000 * 5);
            s.begin();
            nftree_nap.recovery();
            uint64_t rt = s.end();
            printf("recovery time: %ld ms\n", rt / 1000 /1000);
          }
#endif
        },
        i);
  }

  for (auto &t : threads) {
    t.join();
  }

  // printf("update time: %ld\n", update_counter.load());

#ifdef ENABLE_NAP
  nftree_nap.show_statistics();
#endif

  for (size_t t = 0; t < thread_num; ++t) {
    inserted += THREADS[t].inserted;
    found += THREADS[t].found;
    unfound += THREADS[t].unfound;
  }

  printf("Read operations: %ld found, %ld not found\n", found, unfound);
  size_t elapsed[kTestThread];

  for (int i = 0; i < kTestThread; ++i) {
    elapsed[i] =
        static_cast<size_t>((end[i].tv_sec - start[i].tv_sec) * 1000000000ull +
                            (end[i].tv_nsec - start[i].tv_nsec));
  }

  float sec = elapsed[0] / 1000000000.0;

  printf("%f seconds\n", sec);

  float elapsed_sec[kTestThread];
  float all = 0;
  for (int i = 0; i < kTestThread; ++i) {
    elapsed_sec[i] = elapsed[i] / 1000000000.0;

    if (is_test[i]) {
      auto per_thread = READ_WRITE_NUM / thread_num / elapsed_sec[i];
      all += per_thread;
      printf("%f  (%dth threads)\n", per_thread, i);
    }
  }
  printf("%f reqs per second (%ld threads)\n", all, thread_num);

  //   pop.close();

  return 0;
}
