'use strict'

const CharFilter = {
  NFC: 0,
  NFD: 1,
  NFKC: 2,
  NFKD: 3
}

const Comparator = {
  DESCENDING: 0,
  ASCENDING: 1
}

const DataModel	= {
  KV: 0,
  ARRAY: 1,
  MAP: 2
}

const HashingMethod = {
  VIRTUALNODES: 0,
  CONSISTENT: 1,
  UNIFORM: 2,
  RENDEZVOUS: 3
}

const SortBy = {
  RELEVANCE: 0,
  TIMESTAMP: 1
}

const TimeUnit = {
  SECOND: 0,
  MILLISECOND: 1,
  MICROSECOND: 2,
  NANOSECOND: 3
}

const Tokenizer = {
  UNICODE_WORD_BOUNDARIES: 0
}

const TokenStats = {
  NOSTATS: 0,
  UNIQUE: 1,
  FREQUENCY: 2,
  POSITION: 3
}

const TokenTransform = {
  LOWERCASE: 0,
  UPPERCASE: 1,
  CASEFOLD: 2
}

const Type = {
  ROCKSDB: 6
}

const Instruction = {
  INCREMENT: 0,
  OVERWRITE: 7
}

module.exports = {
  CharFilter,
  Comparator,
  DataModel,
  HashingMethod,
  SortBy,
  TimeUnit,
  Tokenizer,
  TokenStats,
  TokenTransform,
  Type,
  Instruction
}
