#include <ydb/core/kqp/common/compile_cache/peephole_cache.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

namespace {

struct TFragment : TPeepholeResult {
    explicit TFragment(size_t bytes) : Bytes(bytes) {}
    ui64 ByteSize() const override { return Bytes; }
    size_t Bytes;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpPeepholeCache) {
    Y_UNIT_TEST(AdmissionPromotionAndLifetime) {
        TKqpPeepholeCache cache(10);
        const auto first = std::make_shared<TFragment>(3);
        cache.Publish({{"a", first}});
        cache.Publish({{"b", std::make_shared<TFragment>(4)}});
        UNIT_ASSERT_EQUAL(cache.Find("a"), first);
        cache.Publish({{"c", std::make_shared<TFragment>(5)}});
        UNIT_ASSERT(!cache.Find("b"));
        UNIT_ASSERT_EQUAL(cache.Find("a"), first);
        cache.Publish({{"a", std::make_shared<TFragment>(11)}, {"d", nullptr}});
        UNIT_ASSERT_EQUAL(cache.Find("a"), first);
        UNIT_ASSERT(!cache.Find("d"));
        auto held = cache.Find("c");
        UNIT_ASSERT(held);
        cache.Clear();
        UNIT_ASSERT(!cache.Find("c"));
        UNIT_ASSERT_VALUES_EQUAL(held->ByteSize(), 5);
    }
}
