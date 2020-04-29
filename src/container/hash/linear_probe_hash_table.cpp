//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "common/logger.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      HashTableHeaderPage* header = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->NewPage(&header_page_id_));
      header->SetSize(num_buckets);
      header->SetPageId(header_page_id_);

      buffer_pool_manager_->UnpinPage(header_page_id_, true);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableHeaderPage* header = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t num_buckets = header->GetSize();
  uint64_t hashVal = hash_fn_.GetHash(key);
  bool rtn = false;

  size_t idx = hashVal % num_buckets;
  size_t offset = hashVal % BLOCK_ARRAY_SIZE;

  page_id_t bucket_id = header->GetBlockPageId(idx);
  table_latch_.RLock();

  if(bucket_id != INVALID_PAGE_ID) {
    HASH_TABLE_BLOCK_TYPE* bucket = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(buffer_pool_manager_->FetchPage(bucket_id));
    size_t iter = offset;

    while(iter < BLOCK_ARRAY_SIZE) {
      if(bucket->IsReadable(iter) && comparator_(bucket->KeyAt(iter), key) == 0) {
        result->push_back(bucket->ValueAt(iter));
        rtn = true;
      }
      iter++;
    }

    buffer_pool_manager_->UnpinPage(bucket_id, false);
  }

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return rtn;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableHeaderPage* header = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t num_buckets = header->GetSize();
  uint64_t hashVal = hash_fn_.GetHash(key);

  size_t idx = hashVal % num_buckets;
  size_t offset = hashVal % BLOCK_ARRAY_SIZE;
  // LOG_DEBUG("hashVal %llu, num_buckets %zu, idx %zu, offset %zu", hashVal, num_buckets, idx, offset);

  while(header->NumBlocks() <= idx) {
    page_id_t tmp;
    buffer_pool_manager_->NewPage(&tmp);
    header->AddBlockPageId(tmp);
    buffer_pool_manager_->UnpinPage(tmp, false);
  }

  page_id_t bucket_id = header->GetBlockPageId(idx);
  table_latch_.RLock();

  HASH_TABLE_BLOCK_TYPE* bucket = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(buffer_pool_manager_->FetchPage(bucket_id));
  bool ins_suc = false;
  size_t iter = offset;

  while(iter < BLOCK_ARRAY_SIZE && !ins_suc) {
    ins_suc = bucket->Insert(iter, key, value);
    if(bucket->IsReadable(iter)) {
      if(comparator_(bucket->KeyAt(iter), key) == 0 && bucket->ValueAt(iter) == value) {
        break;
      }
    }
    iter++;
  }

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bucket_id, true);
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  return ins_suc;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableHeaderPage* header = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t num_buckets = header->GetSize();
  uint64_t hashVal = hash_fn_.GetHash(key);

  size_t idx = hashVal % num_buckets;
  size_t offset = hashVal % BLOCK_ARRAY_SIZE;

  page_id_t bucket_id = header->GetBlockPageId(idx);
  table_latch_.RLock();
  HASH_TABLE_BLOCK_TYPE* bucket = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(buffer_pool_manager_->FetchPage(bucket_id));

  if(bucket_id == INVALID_PAGE_ID) return false;
  else {
    while(offset < BLOCK_ARRAY_SIZE) {
      if(bucket->IsReadable(offset) && comparator_(bucket->KeyAt(offset), key) == 0 && bucket->ValueAt(offset) == value) {
        bucket->Remove(offset);
        break;
      }
      else offset++;
    }
  }

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bucket_id, true);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return offset < BLOCK_ARRAY_SIZE;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  return;
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  HashTableHeaderPage* header = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t cnt = header->NumBlocks();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return cnt;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
