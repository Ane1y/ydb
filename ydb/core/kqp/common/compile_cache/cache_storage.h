#pragma once

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

// LRU + TTL over TLRUList, same layout as TKqpQueryCache. Budget is in TSizeProvider::Size
// units. Not thread-safe; the owner locks and maintains its own secondary indexes from the
// returned evictions.
template <typename TKey, typename TValue, typename TSizeProvider>
class TCacheStorage {
public:
    struct TEvicted {
        TKey Key;
        TValue Value;
    };

    // maxSize is in TSizeProvider units; TLRUList keeps at least one entry. ttl == Zero disables expiry.
    TCacheStorage(size_t maxSize, TDuration ttl)
        : MaxSize(maxSize)
        , Ttl(ttl)
        , List(MaxSize)
    {}

    TMaybe<TValue> Find(const TKey& key, bool promote, TInstant now) {
        auto it = Index.find(TItem(key));
        if (it == Index.end()) {
            return Nothing();
        }
        TItem* item = &const_cast<TItem&>(*it);
        if (Ttl != TDuration::Zero() && item->Value.ExpiredAt <= now) {
            return Nothing();
        }
        if (promote) {
            item->Value.ExpiredAt = now + Ttl;
            List.Promote(item);
        }
        return item->Value.Value;
    }

    // Returns the replaced entry (if any) and everything evicted by the budget.
    TVector<TEvicted> Insert(const TKey& key, TValue value, TInstant now) {
        TVector<TEvicted> out;
        if (auto replaced = Erase(key)) {
            out.push_back(std::move(*replaced));
        }
        auto inserted = Index.emplace(key, TEntry{std::move(value), now + Ttl});
        Y_ABORT_UNLESS(inserted.second);
        TItem* item = &const_cast<TItem&>(*inserted.first);
        // TLRUList evicts at most one item per insert.
        for (TItem* evicted = List.Insert(item); evicted; evicted = List.RemoveIfOverflown()) {
            out.push_back(Drop(evicted));
        }
        return out;
    }

    TMaybe<TEvicted> Erase(const TKey& key) {
        auto it = Index.find(TItem(key));
        if (it == Index.end()) {
            return Nothing();
        }
        TItem* item = &const_cast<TItem&>(*it);
        List.Erase(item);
        return Drop(item);
    }

    TVector<TEvicted> EraseExpired(TInstant now) {
        TVector<TEvicted> out;
        if (Ttl == TDuration::Zero()) {
            return out;
        }
        while (List.GetSize() && List.GetOldest()->Value.ExpiredAt <= now) {
            TItem* item = List.GetOldest();
            List.Erase(item);
            out.push_back(Drop(item));
        }
        return out;
    }

    void Clear() {
        Index.clear();   // item destructors unlink from the list; counters need the reset below
        List = TList(MaxSize);
    }

    size_t Size() const { return Index.size(); }
    size_t GetTotalSize() const { return List.GetTotalSize(); }

private:
    struct TEntry {
        TValue Value;
        TInstant ExpiredAt;
    };

    struct TEntrySize {
        size_t operator()(const TEntry& entry) const {
            return TSizeProvider::Size(entry.Value);
        }
    };

    using TList = TLRUList<TKey, TEntry, TEntrySize>;
    using TItem = typename TList::TItem;
    using TIndex = THashSet<TItem, typename TItem::THash, typename TItem::TEqualTo>;

    TEvicted Drop(TItem* item) {  // item must already be unlinked from the list
        TEvicted evicted{item->Key, std::move(item->Value.Value)};
        auto it = Index.find(*item);
        Y_ABORT_UNLESS(it != Index.end());
        Index.erase(it);
        return evicted;
    }

    const size_t MaxSize;
    const TDuration Ttl;

    TIndex Index;
    TList List;
};

} // namespace NKikimr::NKqp
