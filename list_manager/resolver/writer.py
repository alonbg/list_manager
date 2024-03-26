from typing import Optional
from itertools import groupby
from operator import itemgetter
from itertools import combinations
from wrapt import synchronized
from .abstract import AsyncBatchWriter
from .utils import AsyncJsonFileWriter, ResolverSet
from .. import log, DataSet, Stats


class AsyncResolverCacheWriter(AsyncBatchWriter, AsyncJsonFileWriter):
    def __init__(self):
        AsyncJsonFileWriter.__init__(self, json_file='dns_resolver_cache.json')
        AsyncBatchWriter.__init__(self)
        self.domain_sets:dict[str, set[str]] = DataSet(self.read() if self.file.exists() else ())
        del self.domain_sets['stats']
        
    def sanity(self):
        for left, right in list(combinations([*ResolverSet], 2)):
            try:
                overlap = len(self.get_set(left).intersection(self.get_set(right)))
                assert(overlap == 0)
            except AssertionError:
                log.exception(
    f"error with {left} {len(self.get_set(left))} overlaps {right}, {len(self.get_set(right))} by {overlap} elements")


    # from given set remove difference with other sets
    @synchronized
    def balance(self, target_set=ResolverSet.none):
        #ResolverSet
        other_sets = list(zip(*filter(lambda pair: pair[0] != str(target_set), self.get_sets().items())))
        self.get_set(target_set).difference_update(set.union(*other_sets[1]))

    @synchronized
    def update(self, domains) -> None:
        self.domain_sets[str(ResolverSet.none)].update(self.difference(domains))
 
    @synchronized
    def intersection_update(self, domains: set[str]):
        for s in self.get_sets().values():
            s.intersection_update(domains)

    def difference(self, domains:set[str]) -> set[str]:
        return domains.difference(*self.get_sets().values())
        
    def stats(self) -> dict[str, int]:
        return Stats([(e, len(s)) for e, s in self.get_sets().items()])

    def get_set(self, e:ResolverSet) -> set[str]:
        return self.domain_sets[e]
       
    def get_sets(self, sets:set[ResolverSet]={*ResolverSet}, exclude=False) -> dict[str, set[str]]:
        if exclude:
            sets = {*ResolverSet} - set(sets)
        
        return DataSet([(e, self.get_set(e)) for e in sets])

    def find_set(self, domain) -> Optional[ResolverSet]:
        for e in ResolverSet:
            if domain in self.get_set(e):
                return e

    @synchronized 
    def write(self):
        self.domain_sets['stats'] = self.stats()
        self.domain_sets.move_to_end('stats', last=False)
        super(AsyncJsonFileWriter,).write(self.domain_sets)
        del self.domain_sets['stats']

    async def write_batch(self, batch:list[(ResolverSet, str)]):
        for e, g in groupby(batch, itemgetter(0)):
            res = set(map(itemgetter(1),g))
            for s in self.get_sets([e], exclude=True).values():
                s.difference_update(res)
            self.get_set(e).update(res)
                
        # add stats at the beginning
        self.domain_sets['stats'] = self.stats()
        self.domain_sets.move_to_end('stats', last=False)
        await super().write(self.domain_sets)
        del self.domain_sets['stats']