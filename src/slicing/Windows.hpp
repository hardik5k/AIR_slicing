#pragma once  

#include <unordered_map>
#include "SliceManager.cpp"

extern std::unordered_map<int, PairedWindow*> CURRENT_WINDOWS;
extern int TOTAL_AGG;
extern int PARTIAL_AGG;
extern int FULL_AGG;