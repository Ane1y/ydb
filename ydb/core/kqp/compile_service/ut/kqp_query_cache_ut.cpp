#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NKqp {

namespace {

// TKqpQueryCache reads TAppData::TimeProvider; the fixture swaps it for a manual clock.
class TManualClock : public ITimeProvider {
public:
    TInstant Now() override {
        return Current;
    }

    void Advance(TDuration delta) {
        Current += delta;
    }

private:
    TInstant Current = TInstant::Seconds(1000);
};

struct TClockFixture {
    TClockFixture()
        : Clock(MakeIntrusive<TManualClock>())
        , Saved(TAppData::TimeProvider)
    {
        TAppData::TimeProvider = Clock;
    }

    ~TClockFixture() {
        TAppData::TimeProvider = Saved;
    }

    TIntrusivePtr<TManualClock> Clock;
    TIntrusivePtr<ITimeProvider> Saved;
};

TKqpQueryId MakeQueryId(const TString& text) {
    return TKqpQueryId("db", "/Root", "", "user@builtin", text,
        TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_DML), nullptr, TGUCSettings());
}

// `planBytes` controls PreparedQuery->ByteSize(), the unit the cache accounts in Bytes().
TKqpCompileResult::TConstPtr MakeResult(const TString& uid, const TString& text, size_t planBytes = 16) {
    auto result = TKqpCompileResult::Make(uid, Ydb::StatusIds::SUCCESS, NYql::TIssues(), ETableReadType::Other,
        TDuration::Zero(), MakeQueryId(text));
    auto* proto = new NKikimrKqp::TPreparedQuery();
    proto->SetText(TString(planBytes, 'x'));
    result->PreparedQuery = std::make_shared<TPreparedQueryHolder>(proto, nullptr, /*noFillTables=*/true);
    return result;
}

ui64 PlanBytes(const TKqpCompileResult::TConstPtr& result) {
    return result->PreparedQuery->ByteSize();
}

TVector<TString> SnapshotUids(const TKqpQueryCache& cache) {
    TVector<TString> uids;
    for (const auto& entry : cache.GetSnapshot()) {
        uids.push_back(entry.CompileResult->Uid);
    }
    return uids;
}

constexpr bool NoAstCache = false;
constexpr bool NotPerStatement = false;

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryCache) {

    Y_UNIT_TEST(QueryTextOwnsOneEntry) {
        TClockFixture clock;
        TKqpQueryCache cache(/*size=*/10, TDuration::Minutes(1));

        // A fresh insert is found by uid and by query text, and accounted in bytes and the snapshot.
        auto first = MakeResult("u1", "SELECT 1", 10);
        UNIT_ASSERT(!cache.Insert(first, NoAstCache, NotPerStatement));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(first));
        UNIT_ASSERT_EQUAL(cache.FindByUid("u1", false), first);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), first);
        UNIT_ASSERT(!cache.FindByUid("missing", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), TVector<TString>{"u1"});

        // The same text under a new uid replaces the old entry.
        auto second = MakeResult("u2", "SELECT 1", 30);
        UNIT_ASSERT(cache.Insert(second, NoAstCache, NotPerStatement));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(second));
        UNIT_ASSERT(!cache.FindByUid("u1", false));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), second);
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), TVector<TString>{"u2"});

        // Reinserting the same uid replaces the entry without losing the query -> uid mapping.
        auto third = MakeResult("u2", "SELECT 1", 50);
        UNIT_ASSERT(cache.Insert(third, NoAstCache, NotPerStatement));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(third));
        UNIT_ASSERT_EQUAL(cache.FindByUid("u2", false), third);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), third);

        // Replace swaps the result under an existing uid only.
        auto replaced = MakeResult("u2", "SELECT 1", 70);
        cache.Replace(replaced);
        cache.Replace(MakeResult("missing", "SELECT 2"));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_EQUAL(cache.FindByUid("u2", false), replaced);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), replaced);
        UNIT_ASSERT_EQUAL(cache.GetSnapshot().front().CompileResult, replaced);
    }

    Y_UNIT_TEST(CapacityEvictsLeastRecentlyUsed) {
        TClockFixture clock;
        TKqpQueryCache cache(/*size=*/2, TDuration::Minutes(1));

        auto a = MakeResult("a", "SELECT 1");
        auto b = MakeResult("b", "SELECT 2");
        auto c = MakeResult("c", "SELECT 3");
        cache.Insert(a, NoAstCache, NotPerStatement);
        cache.Insert(b, NoAstCache, NotPerStatement);

        // A promoting lookup moves `a` to the front, so the overflow drops `b`.
        UNIT_ASSERT(cache.FindByUid("a", /*promote=*/true));
        UNIT_ASSERT(cache.Insert(c, NoAstCache, NotPerStatement));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + PlanBytes(c));
        UNIT_ASSERT(!cache.FindByUid("b", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), a);
        UNIT_ASSERT_EQUAL(cache.FindByUid("c", false), c);
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), (TVector<TString>{"a", "c"}));
    }

    Y_UNIT_TEST(TtlSweepDropsExpiredAndPromoteExtends) {
        TClockFixture clock;
        TKqpQueryCache cache(/*size=*/10, TDuration::Seconds(10));

        auto a = MakeResult("a", "SELECT 1");
        auto b = MakeResult("b", "SELECT 2");
        cache.Insert(a, NoAstCache, NotPerStatement);
        cache.Insert(b, NoAstCache, NotPerStatement);

        // A promote at +5s pushes `a` to +15s; `b` still expires at +10s.
        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT(cache.FindByUid("a", /*promote=*/true));
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 0);

        clock.Clock->Advance(TDuration::Seconds(6));   // +11s
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), a);
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), TVector<TString>{"a"});

        clock.Clock->Advance(TDuration::Seconds(5));   // +16s
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
    }

    Y_UNIT_TEST(ReplayMessageAccountingAndEraseByUid) {
        TClockFixture clock;
        TKqpQueryCache cache(/*size=*/10, TDuration::Minutes(1));

        auto a = MakeResult("a", "SELECT 1");
        cache.Insert(a, NoAstCache, NotPerStatement);

        // The attached message is counted in bytes and handed out at most once per timeout.
        const TString message = "replay-message";
        const auto timeout = TDuration::Seconds(30);
        cache.AttachReplayMessage("a", message);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + message.size());
        UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), "");   // just attached
        clock.Clock->Advance(timeout + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), message);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), "");   // within the timeout again
        UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("missing", timeout), "");

        // Re-attaching swaps the accounted size, not adds to it.
        const TString shorter = "short";
        cache.AttachReplayMessage("a", shorter);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + shorter.size());

        // Erase drops the entry, its message bytes and both indexes; a second erase is a no-op.
        UNIT_ASSERT(cache.EraseByUid("a"));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
        UNIT_ASSERT(!cache.FindByUid("a", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 1"), false));
        UNIT_ASSERT(cache.GetSnapshot().empty());
        UNIT_ASSERT(!cache.EraseByUid("a"));
    }

    Y_UNIT_TEST(PerStatementEntriesAndClear) {
        TClockFixture clock;
        TKqpQueryCache cache(/*size=*/10, TDuration::Minutes(1));

        // Per-statement entries share a text but are not indexed by it, so neither replaces the other.
        auto a = MakeResult("a", "SELECT 1");
        auto b = MakeResult("b", "SELECT 1");
        cache.Insert(a, NoAstCache, /*isPerStatementExecution=*/true);
        UNIT_ASSERT(!cache.Insert(b, NoAstCache, /*isPerStatementExecution=*/true));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), a);
        UNIT_ASSERT_EQUAL(cache.FindByUid("b", false), b);
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 1"), false));

        // Clear drops everything and leaves a reusable cache.
        cache.AttachReplayMessage("a", "replay");
        cache.Clear();
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
        UNIT_ASSERT(cache.GetSnapshot().empty());
        UNIT_ASSERT(!cache.FindByUid("a", false));

        auto c = MakeResult("c", "SELECT 3");
        cache.Insert(c, NoAstCache, NotPerStatement);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 3"), false), c);
    }
}

} // namespace NKikimr::NKqp
