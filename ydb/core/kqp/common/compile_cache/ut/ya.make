UNITTEST_FOR(ydb/core/kqp/common/compile_cache)

SRCS(
    compile_cache_registry_ut.cpp
    kqp_cache_ut.cpp
)

PEERDIR(
    ydb/core/kqp/common/compile_cache
)

YQL_LAST_ABI_VERSION()

END()
