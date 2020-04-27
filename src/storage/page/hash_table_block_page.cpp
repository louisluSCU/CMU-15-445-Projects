//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  MappingType result;
  memcpy(&result, array_ + bucket_ind, sizeof(MappingType));
  return result.first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  MappingType result;
  memcpy(&result, array_ + bucket_ind, sizeof(MappingType));
  return result.second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if(IsOccupied(bucket_ind)) return false;

  size_t idx = bucket_ind / 8;
  size_t bit = bucket_ind % 8;
  occupied_[idx] |= (1<<bit);
  readable_[idx] |= (1<<bit);

  MappingType tmp = MappingType(key, value);
  memcpy(array_+bucket_ind, &tmp, sizeof(MappingType));
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  size_t idx = bucket_ind / 8;
  size_t bit = bucket_ind % 8;
  if(idx >= sizeof(occupied_)/sizeof(std::atomic_char)) return;
  readable_[idx] &= ~(1<<bit);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  size_t idx = bucket_ind / 8;
  size_t bit = bucket_ind % 8;
  if(idx >= sizeof(occupied_)/sizeof(std::atomic_char)) return false;
  bool result = occupied_[idx] & (1<<bit);
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  size_t idx = bucket_ind / 8;
  size_t bit = bucket_ind % 8;
  if(idx >= sizeof(readable_)/sizeof(std::atomic_char)) return false;
  bool result = readable_[idx] & (1<<bit);
  return result;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
