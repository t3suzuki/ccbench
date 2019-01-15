#include <atomic>
#include <bitset>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdlib.h>
#include <sys/syscall.h> // syscall(SYS_gettid),
#include <sys/types.h> // syscall(SSY_gettid),
#include <unistd.h> // syscall(SSY_gettid),

#include "include/common.hpp"
#include "include/debug.hpp"
#include "include/procedure.hpp"
#include "include/random.hpp"
#include "include/tuple.hpp"

bool
chkClkSpan(uint64_t &start, uint64_t &stop, uint64_t threshold)
{
	uint64_t diff = 0;
	diff = stop - start;
	if (diff > threshold) return true;
	else return false;
}

void
displayDB()
{
	Tuple *tuple;

	for (unsigned int i = 0; i < TUPLE_NUM; i++) {
		tuple = &Table[i % TUPLE_NUM];
		cout << "------------------------------" << endl;	// - 30
		cout << "key:	" << tuple->key << endl;
		cout << "val:	" << tuple->val << endl;
		cout << endl;
	}
}

void
displayPRO(Procedure *pro)
{
	for (unsigned int i = 0; i < MAX_OPE; ++i) {
		cout << "(ope, key, val) = (";
		switch (pro[i].ope) {
			case Ope::READ:
				cout << "READ";
				break;
			case Ope::WRITE:
				cout << "WRITE";
				break;
			default:
				break;
	}
		cout << ", " << pro[i].key
			<< ", " << pro[i].val << ")" << endl;
	}
}

void
makeDB()
{
	Tuple *tmp;
	Xoroshiro128Plus rnd;
	rnd.init();

	try {
		if (posix_memalign((void**)&Table, 64, TUPLE_NUM * sizeof(Tuple)) != 0) ERR;
	} catch (bad_alloc) {
		ERR;
	}

	for (unsigned int i = 0; i < TUPLE_NUM; i++) {
		tmp = &Table[i];
		tmp->key = i;
		tmp->val = rnd.next() % (TUPLE_NUM * 10);
	}
}

void
makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd)
{
	for (unsigned int i = 0; i < MAX_OPE; ++i) {
		if ((rnd.next() % 10) < RRATIO) {
			pro[i].ope = Ope::READ;
		} else {
			pro[i].ope = Ope::WRITE;
		}
		pro[i].key = rnd.next() % TUPLE_NUM;
		pro[i].val = rnd.next() % TUPLE_NUM;
	}
}

void
setThreadAffinity(int myid)
{
  pid_t pid = syscall(SYS_gettid);
  cpu_set_t cpu_set;

	CPU_ZERO(&cpu_set);
	CPU_SET(myid % sysconf(_SC_NPROCESSORS_CONF), &cpu_set);

	if (sched_setaffinity(pid, sizeof(cpu_set_t), &cpu_set) != 0)
    ERR;
  return;
}

void
waitForReadyOfAllThread()
{
	unsigned int expected, desired;
	expected = Running.load(std::memory_order_acquire);
	do {
		desired = expected + 1;
	} while (!Running.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire));

	while (Running.load(std::memory_order_acquire) != THREAD_NUM);
  return;
}
