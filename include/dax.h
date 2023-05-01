#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

bool dax_enabled = false;
static size_t dax_used = 0;

#if USE_MEMKIND
#include "../third_party/memkind/include/memkind.h"
struct memkind *pmem_kind = NULL;
#endif

#if DAX
static char *dax_base;
void
dax_init()
{
#define MMAP_SIZE ((size_t)1024*1024*1024*512)
#if USE_MEMKIND
  char path[] = "/dev/dax0.0";
  int fd = open(path, O_RDWR);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  dax_base = (char *)mmap(NULL, MMAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, 0);
  int err = memkind_create_fixed(dax_base, MMAP_SIZE, &pmem_kind);
  if (err) {
    exit(-1);
  }
  printf("memkind %s\n", path);
#else
  char path[] = "/dev/dax0.0";
  int fd = open(path, O_RDWR);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  dax_base = (char *)mmap(NULL, MMAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, 0);
  printf("non-memkind %s\n", path);
#endif
  dax_enabled  = true;
  printf("DAX init done\n");
}

#endif

void *
dax_malloc(size_t sz)
{
#if DAX
#if USE_MEMKIND
  return memkind_malloc(pmem_kind, sz);
#else
  size_t offset = __sync_fetch_and_add(&dax_used, sz);
  //printf("%s %d %ld %ld %lx\n", __func__, __LINE__, sz, offset, dax_base + offset);
  return dax_base + offset;
#endif
#else
  exit(__LINE__);
#endif
}

size_t aligned(size_t x, size_t y)
{
  return ((x + y - 1) / y) * y;
}

void *
dax_malloc_align(size_t sz, std::align_val_t align)
{
#if DAX
  /*
  size_t a = (size_t)align;
  size_t offset;
  while (1) {
    size_t old_val = dax_used;
    offset = aligned(dax_used, a);
    size_t new_val = offset + sz;
    if (__sync_bool_compare_and_swap(&dax_used, old_val, new_val))
      break;
  }
  return dax_base + offset;
  */
  //printf("%d\n", (int)align);
  return dax_malloc(sz);
#else
  exit(__LINE__);
#endif
}

void
dax_free(char *p)
{
#if USE_MEMKIND
  memkind_free(pmem_kind, p);
#else
  // TODO
#endif
}

void
dax_stat()
{
#if USE_MEMKIND
  size_t stats_allocated;
  memkind_update_cached_stats();
  memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ALLOCATED, &stats_allocated);
  printf("memkind alloc: %lf\n", stats_allocated / 1000.0 / 1000.0);
#else
  printf("DAX USED: %f MB\n", dax_used / 1024.0 / 1024.0);
#endif
}

#define _GNU_SOURCE
#include <sched.h>

void setThreadAffinity(int core)
{
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(core, &cpu_set);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
}



