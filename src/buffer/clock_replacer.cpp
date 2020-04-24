//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    clock_hand = 0;
    buffer_size = num_pages;
    ref_f = std::vector<bool>(num_pages, 0);
    in_f = std::vector<bool>(num_pages, 0);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    bool result = false;
    int32_t candi = -1;

    clkh_latch.lock();
    ref_latch.lock();
    in_latch.lock();

    for(size_t i=0; i<buffer_size; i++) {
        int32_t index = (clock_hand + i) % buffer_size;
        if(in_f[index] && !ref_f[index]) {
            clock_hand = (index+1) % buffer_size;
            *frame_id = index;
            in_f[index] = false;
            result = true;
            break;
        }
        else if(in_f[index] && ref_f[index]) {
            ref_f[index] = false;
            candi = candi == -1 ? index : candi;
        }
    }
    if(result == false) {
        if(candi == -1) {
            frame_id = nullptr;
        }
        else {
            clock_hand = (candi+1) % buffer_size;
            *frame_id = candi;
            in_f[candi] = false;
            result = true;
        }
    }

    clkh_latch.unlock();
    ref_latch.unlock();
    in_latch.unlock();

    return result;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    if(frame_id >= int32_t(buffer_size)) return;

    in_latch.lock();
    in_f[frame_id] = false;
    in_latch.unlock();

    return;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    if(frame_id >= int32_t(buffer_size)) return;

    ref_latch.lock();
    in_latch.lock();

    in_f[frame_id] = true;
    ref_f[frame_id] = true;

    ref_latch.unlock();
    in_latch.unlock();

    return;
}

size_t ClockReplacer::Size() {
    size_t counter = 0;

    in_latch.lock();
    for(size_t i=0; i<buffer_size; i++){
        if(in_f[i]) counter++;
    }
    in_latch.unlock();

    return counter;
}

}  // namespace bustub
