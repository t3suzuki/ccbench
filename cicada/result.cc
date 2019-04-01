
#include <iostream>

#include "include/result.hpp"

using std::cout, std::endl;

void
CicadaResult::display_totalGCCounts()
{
  cout << "totalGCCounts :\t\t\t" << totalGCCounts << endl;
}

void
CicadaResult::display_AllCicadaResult(const uint64_t clocks_per_us)
{
  display_totalGCCounts();
  display_AllResult(clocks_per_us);
}

void
CicadaResult::add_localGCCounts(uint64_t gcount)
{
  totalGCCounts += gcount;
}

void
CicadaResult::add_localAllCicadaResult(CicadaResult &other)
{
  add_localAllResult(other);
  add_localGCCounts(other.localGCCounts);
}
