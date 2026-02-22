#pragma once
#include <string>
#include <cstdint>

int64_t now_ms();
std::string iso_time();
void append_jsonl(const std::string& path, const std::string& line);