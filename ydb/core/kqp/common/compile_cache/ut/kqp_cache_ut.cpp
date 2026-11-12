#include <ydb/core/kqp/common/compile_cache/kqp_cache.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

namespace {

struct TOneByte {
    static size_t Size(const int&) { return 1; }
};

struct TOneByteAny {
    template <typename T>
    static size_t Size(const T&) { return 1; }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpCache) {

    Y_UNIT_TEST(StorageHonoursTtl) {
        TCacheStorage<int, int, TOneByte> storage(0, TDuration::Seconds(10));
        const auto t0 = TInstant::Seconds(1000);
        storage.Insert(1, 42, t0);
        UNIT_ASSERT(storage.Find(1, false, t0 + TDuration::Seconds(5)));
        UNIT_ASSERT(!storage.Find(1, false, t0 + TDuration::Seconds(11)));
        UNIT_ASSERT_VALUES_EQUAL(storage.EraseExpired(t0 + TDuration::Seconds(11)).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(storage.Size(), 0);
    }

    Y_UNIT_TEST(SecondaryIndexForgetChecksOwner) {
        struct TOwner : TKqpCache<TOwner, TString, int, TOneByteAny> {};
        TOwner::TSecondaryIndex<TString> index;
        index.Set("q", "u1");
        index.Set("q", "u2");             // same alternate key moved to another entry
        index.Forget("q", "u1");          // a late drop of u1 must not break the live mapping
        UNIT_ASSERT(index.Get("q") && *index.Get("q") == "u2");
        index.Forget("q", "u2");
        UNIT_ASSERT(!index.Get("q"));
    }
}
