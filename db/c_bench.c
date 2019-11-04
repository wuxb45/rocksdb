// mkdir build; cd build; cmake ..; make -j20 c_bench
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>

#include "rocksdb/c.h"

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef  uint8_t  u8;

  static inline u64
xorshift(const u64 seed)
{
  u64 x = seed ? seed : 88172645463325252lu;
  x ^= x >> 12; // a
  x ^= x << 25; // b
  x ^= x >> 27; // c
  return x * 2685821657736338717lu;
}

static __thread u64 __random_seed_u64 = 0;

  static inline u64
random_u64(void)
{
  __random_seed_u64 = xorshift(__random_seed_u64);
  return __random_seed_u64;
}

  static inline u64
time_nsec(void)
{
  struct timespec ts;
  // MONO_RAW is 5x to 10x slower than MONO
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000000lu + ts.tv_nsec;
}

  static inline double
time_sec(void)
{
  const u64 nsec = time_nsec();
  return ((double)nsec) / 1000000000.0;
}

struct kv {
  union {
    char * key;
    u64 * k64;
  };
  size_t klen;
  char * value;
  size_t vlen;
};

  static void
updatekv(struct kv * const kv, const u64 seq, const u64 ep)
{
  sprintf(kv->key, "%08lu", seq);
  for (u64 x = 1; x < 5; x++)
    kv->k64[x] = kv->k64[0];
  sprintf(kv->key+40, "%08lu", ep);
}

  int
main(int argc, char ** argv)
{
  char * err = NULL;
  rocksdb_options_t* const options = rocksdb_options_create();
  rocksdb_options_set_compression(options, 0);
  rocksdb_options_set_create_if_missing(options, 1);

  rocksdb_cache_t* cache = rocksdb_cache_create_lru(1lu<<32);
  rocksdb_block_based_table_options_t* const topt = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(topt, cache);
  rocksdb_block_based_options_set_cache_index_and_filter_blocks(topt, 1);
  rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(topt, 1);
  rocksdb_options_set_block_based_table_factory(options, topt);

  rocksdb_t* db = rocksdb_open(options, "/tmp/crdb", &err);


  rocksdb_writeoptions_t * const wopt = rocksdb_writeoptions_create();
  rocksdb_readoptions_t * const ropt = rocksdb_readoptions_create();
  rocksdb_readoptions_set_fill_cache(ropt, 1);

  struct kv kv;
  kv.key = calloc(1, 4096);
  kv.value = calloc(1, 4096);
  kv.vlen = 200;

  const u64 nkeys = 1e6; // insert 1000000 new keys in each rounds; 100 rounds
  const u64 nseeks = 1e5; // 100000 times
  double t0, t1;
  for (u64 e = 0; e < 100; e++) {
    t0 = time_sec();
    for (u64 i = 0; i < nkeys; i++) {
      updatekv(&kv, i, e); // e in [0..9], i in [0..nkeys]
      rocksdb_put(db, wopt, kv.key, 48, kv.value, kv.vlen, &err);
    }
    t1 = time_sec();
    printf("put e %lu n %lu dt %.3lf\n", e, nkeys, t1-t0);
    sleep(1);
    rocksdb_iterator_t * const iter = rocksdb_create_iterator(db, ropt);
    for (u64 nnext = 1; nnext <= 64; nnext *= 8) {
      for (u64 r = 0; r < 3; r++) { // the last two rounds should have everything cached
        u64 match5 = 0;
        t0 = time_sec();
        for (u64 i = 0; i < nseeks; i++) {
          const u64 ki = random_u64() % nkeys;
          updatekv(&kv, ki, 0); // random prefix
          rocksdb_iter_seek(iter, kv.key, 40);
          size_t sklen;

          // iter can become invalid after seek, further calls will cause segfault
          for (u64 j = 0; (j < nnext) && rocksdb_iter_valid(iter); j++)
            rocksdb_iter_next(iter);

          if (rocksdb_iter_valid(iter)) {
            const char * const skey = rocksdb_iter_key(iter, &sklen);
            if (skey && (memcmp(skey, kv.key, 40) == 0))
              match5++;
          }
        }
        t1 = time_sec();
        double kops = nkeys / 1000 / (t1-t0);
        printf("seek+next%lu e %lu n %lu dt %.3lf kops %.3lf match5 %lu\n",
          nnext, e, nseeks, t1-t0, kops, match5);
      }
    }
    rocksdb_iter_destroy(iter);
  }
  rocksdb_options_destroy(options);
  rocksdb_readoptions_destroy(ropt);
  rocksdb_writeoptions_destroy(wopt);
  rocksdb_close(db);
}
