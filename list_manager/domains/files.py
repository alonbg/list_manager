from datetime import datetime
import re
import copy
from pathlib import Path
from .transfer import Transfer
from .utils import DomainUtils, FileSet
from .. import JsonFile, DataSet, Stats


class DomainsFile(JsonFile, DomainUtils, Transfer):
    def __init__(self, idx: int, category: str, url: str, path: Path):
        self.idx = idx
        self.category = category
        self.url = url
        self.from_cache = False
        super().__init__(path.joinpath(f"{category}_{idx}_{self.url_hash(url)}.json"))

    def get_set(self, e:FileSet) -> set[str]:
         return self.fileSet[e]

    def write(self) -> None:
        self.fileSet['stats'] = self.stats
        self.fileSet.move_to_end('stats', last=False)
        super().write(self.fileSet)
        del self.fileSet['stats']

    def read(self) -> None:
        self.fileSet = DataSet(super().read())
        self.stats = Stats(self.fileSet['stats'])
        del self.fileSet['stats']

        if self.idx != self.stats['idx']:
            raise (IndexError(f"wrong idx {self.file}"))

    @staticmethod
    def decode(data: bytes) -> list[str]:
        return data.decode("utf-8").split("\n")

    @staticmethod
    def encode(domains: set[str]) -> bytes:
        return "\n".join(domains).encode("utf-8")

    def parse(self, force=False) -> None:
        raw_data = self.download()
        if force or not self.from_cache or not self.exists():
            self.stats = Stats([('idx', self.idx), ('category', self.category), ('url', self.url)])
            data = self.decode(raw_data)
            self.fileSet, self.stats['parser'] = self.clean_list(data)
        else:
            self.read()

        # Compile regex patterns
        self.compiled_patterns = [re.compile(p) for p in self.get_set(FileSet.patterns)]

    def check(self):
        if "parser" not in self.stats:
            raise (ValueError(f"{self.category}_{self.idx} must parse list"))

        if "deDup" not in self.stats:
            raise (ValueError(f"{self.category}_{self.idx} must deDup list"))

        if "reduce_wl" not in self.stats:
            raise (ValueError(f"{self.category}_{self.idx} must extract whitelist intersection of list"))

    def download(self) -> bytes:
        body, self.from_cache = self.download_list(self.url)
        return body

    def upload(self, force=False) -> None:
        self.check()
        if force or not self.from_cache:
            payload = self.get_set(FileSet.domains).difference(self.get_set(FileSet.dup_domains)).union(
                self.get_set(FileSet.patterns).difference(self.get_set(FileSet.dup_patterns)))

            if self.upload_list(self.encode(payload), self.url):
                    self.stats["upload"] = str(datetime.now())

    def match_patterns(self, domains) -> set[str]:
        matches = set()
        matched_patterns = set()
        for domain in domains:
            for pattern in self.compiled_patterns:
                if pattern.match(domain):
                    matched_patterns.add((pattern, domain))
                    matches.add(domain)

        return matches

    def __sub__(self, other):
        return copy.deepcopy(self).__isub__(other)

    def __isub__(self, other):
        if not self.stats["parser"]:
            raise (ValueError("must parse list before intersection"))

        de_dup = self.stats.get("deDup", dict())

        # match domains with other patterns
        matched = other.match_patterns(self.get_set(FileSet.domains))
        de_dup[f"{other.category}_{other.idx}_m"] = len(matched)
        self.get_set(FileSet.dup_domains).update(matched)

        # domains string matched by other domains
        intersect = self.get_set(FileSet.domains).intersection(other.get_set(FileSet.domains))
        de_dup[f"{other.category}_{other.idx}_d"] = len(intersect)
        self.get_set(FileSet.dup_domains).update(intersect)

        # domains string matched by other patterns
        intersect = self.get_set(FileSet.patterns).intersection(other.get_set(FileSet.patterns))
        de_dup[f"{other.category}_{other.idx}_p"] = len(intersect)
        self.get_set(FileSet.dup_patterns).update(intersect)
        
        self.stats["deDup"] = de_dup
        return self