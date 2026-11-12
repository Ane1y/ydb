#pragma once

#include "kqp_cache.h"
#include "types.h"

#include <util/system/spinlock.h>

#include <atomic>

namespace NKikimr::NKqp {

struct TByteSize {
    template <typename T>
    static size_t Size(const T& value) { return value ? value->ByteSize() : 0; }
};

class TKqpPeepholeCache final
    : public TKqpCache<TKqpPeepholeCache, TPeepholeKey, TPeepholeResultPtr, TByteSize>
{
    using TBase = TKqpCache<TKqpPeepholeCache, TPeepholeKey, TPeepholeResultPtr, TByteSize>;

public:
    using TBase::TBase;
};

// Node-wide compile caches. The generation is owned by the compile service (its config
// generation); the registry only mirrors it so lookups handed to in-flight compilations miss
// once the owner has moved on.
class TKqpCompileCacheRegistry : public TThrRefBase {
public:
    struct TSettings {
        bool PeepholeEnabled = false;   // off: lookups are null, publications are dropped
        ui64 PeepholeMaxBytes = 64ull << 20;
    };

    explicit TKqpCompileCacheRegistry(const TSettings& settings);

    bool IsPeepholeEnabled() const { return PeepholeEnabled; }
    TKqpPeepholeCache& Peephole() { return PeepholeCache; }

    // Takes the results of one compilation; a no-op while the peephole cache is disabled.
    void PublishPeephole(TPeepholeResults&& results);

    ui64 CurrentGeneration() const {
        return Generation.load(std::memory_order_acquire);
    }

    // Drops everything and records `generation` as current; lookups bound to older generations start missing.
    void Invalidate(ui64 generation);

    // Bound to `generation`: misses once Invalidate() moves to a different one. Null while disabled.
    IPeepholeLookupPtr MakePeepholeLookup(ui64 generation);

private:
    const bool PeepholeEnabled;
    TKqpPeepholeCache PeepholeCache;
    std::atomic<ui64> Generation{0};
};

using TKqpCompileCacheRegistryPtr = TIntrusivePtr<TKqpCompileCacheRegistry>;

} // namespace NKikimr::NKqp
