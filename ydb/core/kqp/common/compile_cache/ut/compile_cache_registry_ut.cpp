#include <ydb/core/kqp/common/compile_cache/compile_cache_registry.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

namespace {

struct TStubPeephole : public TPeepholeResult {
    explicit TStubPeephole(ui64 bytes) : Bytes(bytes) {}
    ui64 ByteSize() const override { return Bytes; }
    ui64 Bytes;
};

TPeepholeResultPtr MakePeephole(ui64 bytes) {
    return std::make_shared<TStubPeephole>(bytes);
}

TPeepholeKey MakeKey(const char* canonical) {
    return TPeepholeKey(canonical);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpCompileCacheRegistry) {

    Y_UNIT_TEST(PeepholeByteBudget) {
        TKqpPeepholeCache cache(/*maxBytes=*/100, TDuration::Zero());

        // Reinserting a key replaces its bytes rather than adding to them.
        cache.Insert(MakeKey("a"), MakePeephole(10));
        cache.Insert(MakeKey("a"), MakePeephole(30));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalSize(), 30);
        UNIT_ASSERT_VALUES_EQUAL(cache.Find(MakeKey("a"), false)->ByteSize(), 30);

        // Overflowing the budget (30 + 40 + 50 > 100) evicts the oldest entry only.
        cache.Insert(MakeKey("b"), MakePeephole(40));
        cache.Insert(MakeKey("c"), MakePeephole(50));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
        UNIT_ASSERT(!cache.Find(MakeKey("a"), true));
        UNIT_ASSERT(cache.Find(MakeKey("b"), true));
        UNIT_ASSERT(cache.Find(MakeKey("c"), true));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalSize(), 90);
    }

    Y_UNIT_TEST(PeepholeFlagAndInvalidation) {
        TPeepholeResults published;
        published.emplace(MakeKey("p"), MakePeephole(1));

        // Disabled: no lookup is handed out and publications are dropped.
        auto disabled = MakeIntrusive<TKqpCompileCacheRegistry>(TKqpCompileCacheRegistry::TSettings{.PeepholeEnabled = false});
        UNIT_ASSERT(!disabled->MakePeepholeLookup(disabled->CurrentGeneration()));
        disabled->PublishPeephole(TPeepholeResults(published));
        UNIT_ASSERT_VALUES_EQUAL(disabled->Peephole().Size(), 0);

        // Enabled: a publication is visible to a lookup of the same generation ...
        auto registry = MakeIntrusive<TKqpCompileCacheRegistry>(TKqpCompileCacheRegistry::TSettings{.PeepholeEnabled = true});
        registry->PublishPeephole(std::move(published));
        auto lookup = registry->MakePeepholeLookup(registry->CurrentGeneration());
        UNIT_ASSERT(lookup && lookup->Find(MakeKey("p")));

        // ... and misses once the owner moves to the next generation.
        const ui64 before = registry->CurrentGeneration();
        registry->Invalidate(before + 1);
        UNIT_ASSERT_VALUES_EQUAL(registry->CurrentGeneration(), before + 1);
        UNIT_ASSERT(!lookup->Find(MakeKey("p")));
        UNIT_ASSERT_VALUES_EQUAL(registry->Peephole().Size(), 0);
    }
}
