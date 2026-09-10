#include "kqp_executer_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql::NDqProto;

TDqComputeActorStats MakeReport(ui64 taskId, ui64 cpu, ui64 memory, ui64 tableBytes, ui64 sourceBytes) {
    TDqComputeActorStats report;
    report.SetMemoryUsage(memory);
    auto& task = *report.AddTasks();
    task.SetTaskId(taskId);
    task.SetCpuTimeUs(cpu);
    task.SetIngressBytes(sourceBytes);
    auto& table = *task.AddTables();
    table.SetTablePath("/Root/Table");
    table.SetReadBytes(tableBytes);
    return report;
}

void Init(TQueryExecutionStats& stats) {
    stats.TaskCount = 2;
    stats.TaskCount4 = 4;
    stats.ComputeCpuTimeUs.Resize(4);
    stats.StartTs = TInstant::Seconds(10);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpCurrentExecutionStats) {
    Y_UNIT_TEST(CollectWithoutFullProfile) {
        for (auto mode : {Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                          Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC}) {
            TQueryExecutionStats stats(mode, nullptr, nullptr, 0);
            Init(stats);
            auto first = MakeReport(1, 100, 4096, 1000, 700);
            auto second = MakeReport(2, 200, 8192, 2000, 1400);
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            stats.UpdateTaskStats(2, 2, second, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            // Reports contain cumulative values, so receiving one twice must not double them.
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            stats.StorageCpuTimeUs = 50;
            auto snapshot = stats.GetCurrentExecStats(TInstant::Seconds(12));
            UNIT_ASSERT_VALUES_EQUAL(snapshot.DurationUs, 2000000);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 350);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 12288);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.TableReadBytes, 3000);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.SourceReadBytes, 2100);
            UNIT_ASSERT(stats.StageStats.empty());

            // Current memory can decrease all the way to zero while the task is running.
            first.SetMemoryUsage(0);
            first.MutableTasks(0)->SetCpuTimeUs(150);
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            snapshot = stats.GetCurrentExecStats(TInstant::Seconds(13));
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 8192);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 400);

            // A terminal report may still contain allocated memory; it is no longer live.
            stats.UpdateTaskStats(2, 2, second, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
            snapshot = stats.GetCurrentExecStats(TInstant::Seconds(14));
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.TableReadBytes, 3000);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.SourceReadBytes, 2100);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 400);
        }
    }

    Y_UNIT_TEST(FailureWithoutTaskStatsReleasesMemory) {
        TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC, nullptr, nullptr, 0);
        Init(stats);
        auto report = MakeReport(1, 100, 4096, 1000, 700);
        stats.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        stats.UpdateTaskStats(1, 1, TDqComputeActorStats{}, nullptr, COMPUTE_STATE_FAILURE, TDuration::Max());
        const auto snapshot = stats.GetCurrentExecStats(TInstant::Seconds(12));
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 100);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.TableReadBytes, 1000);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.SourceReadBytes, 700);
    }
}

} // namespace NKikimr::NKqp
