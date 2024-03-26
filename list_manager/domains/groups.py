from itertools import combinations, product
from typing import Generator
from .files import DomainsFile
from .. import log, Utils


class DomainGroup:
    def __init__(self, category: str, url_list: list[str], wl_type=False):
        url_list = sorted(url_list) # required for consistent hash mapping to file name 
        self.domain_files:list[DomainsFile] = [self._gen(idx, category, url) for idx, url in enumerate(url_list)]
        self.wl_type = wl_type
        self.category = category

    @staticmethod
    def _gen(idx, category, url):
        return DomainsFile(idx, category, url, path=Utils.get_create_dir("domains"))

    def iter_domain_files(self) -> Generator[DomainsFile, None, None]:
        for d in self.domain_files:
            yield d

    def parse(self, **kwargs) -> None:
        for d in self.iter_domain_files():
            d.parse(**kwargs)

    def write(self) -> None:
        for d in self.domain_files:
            d.write()

    def de_dup(self) -> None:
       self.domain_files =[ left - right for left, right in list(combinations(self.domain_files, 2))]

    def upload(self, **kwargs) -> None:
        for d in self.domain_files:
            try:
                d.upload(**kwargs)
            except Exception as e:
                log.error(f"{e}")
                continue

    def get_states(self):
        for d in self.domain_files:
            yield d.stats

    def log_stats(self) -> None:
        for d in self.domain_files:
            print(d.stats)

    def set_stats(self, key, value):
        for d in self.domain_files:
            d.stats[key] = value

    def __isub__(self, other):
        self.domain_files = [left - right for left, right in product(self.domain_files, other.domain_files)]

    def common(self):
        self.parse()
        self.de_dup()
        self.write()
