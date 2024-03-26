from functools import partial
from .abstract import SingletonInst
from .executer import ThreadedAsyncExecuter
from .utils import ResolverSet
from .writer import AsyncResolverCacheWriter
from .processor import AsyncResolveProcessor
from .. import log, DataSet, Stats


class AsyncResolver(AsyncResolverCacheWriter, ThreadedAsyncExecuter, SingletonInst):
    resolvable = {ResolverSet.resolvable, ResolverSet.timeout, ResolverSet.none}
    unresolved = {*ResolverSet} - resolvable
        
    def __init__(self,  **kwargs):
        AsyncResolverCacheWriter.__init__(self)
        ThreadedAsyncExecuter.__init__(self, **kwargs)

    def refresh_cache(self, **kwargs):
        domains = set.union(*[self.get_set(e) for e in self.get_refresh_sets()])
        self.batch_resolve(domains, **kwargs)
        
    def batch_resolve(self, domains: set[str], **kwargs):
        processor_factory = partial(AsyncResolveProcessor, **kwargs)
        self.execute(list(domains), processor_factory=processor_factory, async_writer=self.write_batch)

    def intersect_sets(self, domains: set[str]) -> dict[str, set[str]]:
        return DataSet([(e, domains.intersection(s)) for e, s in self.get_sets().items()])

    def intersect_stats(self, domains: set[str]) -> dict[str, int]:
        return Stats([(e, len(s)) for e, s in self.intersect_sets(domains).items()])

    @classmethod
    def get_refresh_sets(cls) -> set[ResolverSet]:
        return set([*ResolverSet]) - set([ResolverSet.resolvable, ResolverSet.unresolvable])

    def get_resolvable(self, domains: set[str]) -> set[str]:
        return domains.intersection(set.union(*[self.get_set(e) for e in self.resolvable]))

    def get_unresolved(self, domains: set[str]) -> set[str]:
        return domains.intersection(set.union(*[self.get_set(e) for e in {*ResolverSet} - self.resolvable]))
    
    # def merge_generator(self) -> Generator[dict[str, int], set[str], None]:
    #     try:
    #         while True:
    #             domains = (yield)
    #             self.update(domains)
    #             yield self.intersect_stats(domains)
    #     except GeneratorExit as e:
    #         logging.info(f"Generator closing...{e}")
    
    # def diff_sets(self, domains: set[str]) -> dict[str, set[str]]:
    #     return IntEnumDict([(e, domains.difference(s)) for e, s in self.cache_sets().items()])
    
    # def diff_stats(self, domains: set[str]) -> dict[str, int]:
    #     stats = Stats([(e, len(s)) for e, s in self.diff_sets(domains).items()])
    #     return stats
