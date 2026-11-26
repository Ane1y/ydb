#pragma once

#include "kqp_cache.h"
#include "types.h"

namespace NKikimr::NKqp {

struct TPeepholeCacheEntry {
    TPeepholeResultPtr Result;
    size_t Bytes = 0;

    struct TWeight {
        size_t operator()(const TPeepholeCacheEntry& entry) const { return entry.Bytes; }
    };
};

class TKqpPeepholeCache final
    : public IPeepholeLookup
    , private TKqpCache<TKqpPeepholeCache, TPeepholeKey, TPeepholeCacheEntry, TPeepholeCacheEntry::TWeight>
{
    using TBase = TKqpCache<TKqpPeepholeCache, TPeepholeKey, TPeepholeCacheEntry, TPeepholeCacheEntry::TWeight>;
    friend TBase;

public:
    explicit TKqpPeepholeCache(size_t maxBytes)
        : TBase(maxBytes, TDuration::Zero())
    {}

    using TBase::Clear;

    TPeepholeResultPtr Find(const TPeepholeKey& key) override {
        return TBase::Find(key, true).Result;
    }

    void Publish(TPeepholeResults&& results) {
        for (auto& [key, result] : results) {
            if (result) {
                const size_t bytes = result->ByteSize();
                Insert(key, {std::move(result), bytes});
            }
        }
    }

private:
    // This cache has no TTL or access timestamps.
    static TInstant Now() { return TInstant::Zero(); }
    void OnDropped(const TEvicted&) {}
    void OnCleared() {}
};

} // namespace NKikimr::NKqp
