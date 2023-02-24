#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

bool dax_enabled = false;
static size_t dax_used = 0;

#if DAX
static char *dax_base;
void
dax_init()
{
  char path[] = "/dev/dax0.0";
  int fd = open(path, O_RDWR);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
#define MMAP_SIZE ((size_t)1024*1024*1024*128)
  dax_base = (char *)mmap(NULL, MMAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, 0);
  dax_enabled  = true;
}

#endif

void *
dax_malloc(size_t sz)
{
#if DAX
  size_t offset = __sync_fetch_and_add(&dax_used, sz);
  //printf("%s %d %ld\n", __func__, __LINE__, sz);
  return dax_base + offset;
#else
  exit(__LINE__);
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



