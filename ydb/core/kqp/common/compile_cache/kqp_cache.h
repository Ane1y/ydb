#pragma once

#include "cache_storage.h"

#include <util/system/spinlock.h>

namespace NKikimr::NKqp {

// Thread-safe owner over TCacheStorage (CRTP, no virtual calls). TDerived may define
// OnDropped/OnCleared (called under Lock) to maintain secondary indexes and use the *Locked
// methods under its own guard; it must befriend the base so the hooks can stay private.
template <typename TDerived, typename TKey, typename TValue, typename TSizeProvider>
class TKqpCache {
public:
    using TStorage = TCacheStorage<TKey, TValue, TSizeProvider>;
    using TEvicted = typename TStorage::TEvicted;

    // Alternate key -> primary key. Owned by TDerived and maintained from its hooks under Lock.
    // Forget() drops the mapping only if it still points to the given primary key.
    template <typename TAltKey>
    class TSecondaryIndex {
    public:
        void Set(const TAltKey& alt, const TKey& key) {
            if (auto it = Map.find(alt); it != Map.end()) {
                it->second = key;
            } else {
                Map.emplace(alt, key);
            }
        }

        const TKey* Get(const TAltKey& alt) const {
            auto it = Map.find(alt);
            return it == Map.end() ? nullptr : &it->second;
        }

        void Forget(const TAltKey& alt, const TKey& key) {
            if (auto it = Map.find(alt); it != Map.end() && it->second == key) {
                Map.erase(it);
            }
        }

        void Clear() { Map.clear(); }

    private:
        THashMap<TAltKey, TKey> Map;
    };

    TKqpCache(size_t maxSize, TDuration ttl)
        : Storage(maxSize, ttl)
    {}

    TValue Find(const TKey& key, bool promote) {
        TGuard<TAdaptiveLock> guard(Lock);
        return FindLocked(key, promote);
    }

    // Replaces under the same key. Returns the number of entries dropped (replaced or evicted).
    size_t Insert(const TKey& key, TValue value) {
        TGuard<TAdaptiveLock> guard(Lock);
        return InsertLocked(key, std::move(value));
    }

    bool Erase(const TKey& key) {
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseLocked(key);
    }

    size_t EraseExpired() {
        TGuard<TAdaptiveLock> guard(Lock);
        auto dropped = Storage.EraseExpired(TDerived::Now());
        for (const auto& evicted : dropped) {
            Self().OnDropped(evicted);
        }
        return dropped.size();
    }

    void Clear() {
        TGuard<TAdaptiveLock> guard(Lock);
        Storage.Clear();
        Self().OnCleared();
    }

    size_t Size() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.Size();
    }

    size_t TotalSize() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.GetTotalSize();
    }

protected:
    TValue FindLocked(const TKey& key, bool promote) {
        auto found = Storage.Find(key, promote, TDerived::Now());
        return found ? *found : TValue();
    }

    size_t InsertLocked(const TKey& key, TValue value) {
        auto dropped = Storage.Insert(key, std::move(value), TDerived::Now());
        for (const auto& evicted : dropped) {
            Self().OnDropped(evicted);
        }
        return dropped.size();
    }

    bool EraseLocked(const TKey& key) {
        auto dropped = Storage.Erase(key);
        if (dropped) {
            Self().OnDropped(*dropped);
        }
        return dropped.Defined();
    }

    // Defaults; TDerived hides them when it keeps secondary indexes or uses another clock.
    void OnDropped(const TEvicted&) {}
    void OnCleared() {}
    static TInstant Now() { return TInstant::Now(); }

    mutable TAdaptiveLock Lock;
    TStorage Storage;

private:
    TDerived& Self() { return static_cast<TDerived&>(*this); }
};

} // namespace NKikimr::NKqp
