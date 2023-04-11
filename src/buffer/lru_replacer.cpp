//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : mlock() { this->container = new std::list<frame_id_t>; }

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(this->mlock);
  auto c = this->container;
  if (nullptr == c || c->empty()) {
    return false;
  }
  *frame_id = c->back();
  c->pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(this->mlock);
  auto c = this->container;
  if (nullptr == c || c->empty()) {
    return;
  }
  for (auto it = c->begin(); it != c->end(); it++) {
    if (*it == frame_id) {
      c->erase(it);
      break;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(this->mlock);
  auto c = this->container;
  if (nullptr == c) {
    return;
  }
  auto item = std::find(c->begin(), c->end(), frame_id);
  if (item != c->end()) {
    return;
  }
  c->push_front(frame_id);
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(this->mlock);
  auto c = this->container;
  if (nullptr == c || c->empty()) {
    return 0;
  }
  int r = c->size();
  return r;
}

}  // namespace bustub
