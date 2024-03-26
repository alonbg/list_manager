from .domains import Binder
from .resolver import AsyncResolver
from . import log

class Runner:
    resolver = AsyncResolver()
    binder = Binder()
    
    def update_resolver(self) -> None:
        for d in self.binder.files_iter():
            self.resolver.update(d.fileSet['domains'])
            d.stats['cache'] = self.resolver.intersect_stats(d.fileSet['domains'])
            assert(sum(d.stats['cache'].values()) == len(d.fileSet['domains']) == d.stats['parser']['domains'])

    def compact_resolver(self):
        all_domains = set()
        for d in self.files_iter():
            all_domains.update(d.fileSet['domains'])
        try:
            assert(len(self.resolver.difference(all_domains)) == 0)
        except AssertionError as e:
            log.exception(f"{e}")
            self.resolver.intersection_update(all_domains)
            assert(len(self.resolver.difference(all_domains)) == 0)
            

    def run(self):
        #print(f"\n{'category' : <15} {'wl_type' : <7} count")
        # print(f"{g.category : <15} {g.wl_type : <7} files: {len(g.domain_files)}")
        # for g in self.binder.group_iter():
        #     g.parse()
        #     g.deDup()
        #     if g.wl_type:
        #         g.set_stats("reduce_wl", True)
 
        # self.binder.reduce_wl()
        # self.update_resolver()
        # self.compact_resolver()
        self.resolver.refresh_cache(max_concurrent_tasks=60, batch_size=50)
         
        # for g in self.binder.group_iter():
        #     g.upload()


def main():
    global mock_upload
    mock_upload = True
    r = Runner()
    r.run()

if __name__ == "__main__":
    main()



