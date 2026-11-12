#include "compile_cache_registry.h"

namespace NKikimr::NKqp {

namespace {

class TPeepholeLookup : public IPeepholeLookup {
public:
    TPeepholeLookup(TKqpCompileCacheRegistryPtr cache, ui64 generation)
        : Cache(std::move(cache))
        , Generation(generation)
    {}

    TPeepholeResultPtr Find(const TPeepholeKey& key) override {
        if (Cache->CurrentGeneration() != Generation) {
            return nullptr;
        }
        return Cache->Peephole().Find(key, /*promote=*/true);
    }

private:
    const TKqpCompileCacheRegistryPtr Cache;
    const ui64 Generation;
};

} // anonymous namespace

// TKqpCompileCacheRegistry

TKqpCompileCacheRegistry::TKqpCompileCacheRegistry(const TSettings& settings)
    : PeepholeEnabled(settings.PeepholeEnabled)
    , PeepholeCache(settings.PeepholeMaxBytes, TDuration::Zero())
{}

void TKqpCompileCacheRegistry::PublishPeephole(TPeepholeResults&& results) {
    if (!PeepholeEnabled) {
        return;
    }
    for (auto& [key, result] : results) {
        if (result) {
            PeepholeCache.Insert(key, std::move(result));
        }
    }
}

void TKqpCompileCacheRegistry::Invalidate(ui64 generation) {
    // Publish the generation first: in-flight lookups must start missing before the stores are emptied.
    Generation.store(generation, std::memory_order_release);
    PeepholeCache.Clear();
}

IPeepholeLookupPtr TKqpCompileCacheRegistry::MakePeepholeLookup(ui64 generation) {
    if (!PeepholeEnabled) {
        return nullptr;
    }
    return std::make_shared<TPeepholeLookup>(TKqpCompileCacheRegistryPtr(this), generation);
}

} // namespace NKikimr::NKqp
