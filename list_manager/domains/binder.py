import os
from typing import Generator
from .files import DomainsFile
from .groups import DomainGroup
from .. import log, JsonFile

class Binder:
    BL_CONFIG_JSON = os.environ.get("BL_CONFIG_JSON", "bl_config.json")
    
    def __init__(self):
        groups = self.load()
        self.wl_categories = groups["wl_categories"]
        self.bl_categories = groups["bl_categories"]
        self.groups = groups

    @classmethod
    def load(cls) -> dict[str, list[DomainGroup]]:
        data = JsonFile(cls.BL_CONFIG_JSON, root=False).read()
        return {
            "bl_categories": [DomainGroup(cat, url_list) for (cat, url_list) in data["blackLists"].items()],
            "wl_categories": [DomainGroup(cat, url_list, wl_type=True) for (cat, url_list) in data["whiteLists"].items()],
        }

    def group_iter(self) -> Generator[DomainGroup, None, None]:
        for g in self.groups["wl_categories"] + self.groups["bl_categories"]:
            yield g
    
    def files_iter(self)-> Generator[DomainsFile, None, None]:
        for g in self.group_iter():
            for d in g.iter_domain_files():
                yield d
 
    def reduce_wl(self):
        for bl in self.groups["bl_categories"]:
            for wl in self.groups["wl_categories"]:
                bl -= wl
            bl.set_stats("reduce_wl", True)

