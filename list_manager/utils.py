import os
import json
from pathlib import Path
from enum import IntEnum
from collections import OrderedDict
import yaml
import logging
from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, tracebacks_show_locals=True)],
)

log = logging.getLogger("lm")


ROOT_DIR = Path(os.environ.get("ROOT_DIR", Path.cwd().joinpath(".temp"))).absolute()

class IntEnumDict(OrderedDict):
    def __setitem__(self, key, item) -> None:
         if isinstance(key, IntEnum):
                key = key.name     
         return super().__setitem__(key, item)
    
    def __getitem__(self, key):
        if isinstance(key, IntEnum):
            key = key.name
        return super().__getitem__(key)


class DataSet(IntEnumDict):
    def __missing__(self, key):
        return set()  # if key not found

class Stats(IntEnumDict):
    def __missing__(self, key):
        return 0  # if key not found
    
    def __str__(self):
       return f'\n{yaml.dump(dict(self))}'
    
class Utils:
    @staticmethod
    def with_root(file_path:str) -> Path:
        file_path:Path = Path(file_path)
        if file_path.is_absolute():
            file_path.relative_to(ROOT_DIR)
            return file_path
        else:
            return  ROOT_DIR.joinpath(file_path)
 
    @classmethod
    def get_create_dir(cls, dir_path:str) -> Path:
         dir = cls.with_root(dir_path)
         dir.mkdir(parents=True, exist_ok=True)
         return dir


class JsonFile:    
    def __init__(self, json_file:str, root=True):
        self.file = Utils.with_root(json_file) if root else Path(json_file)

    def exists(self):
        return self.file.exists()
    
    def read(self) -> dict:
        def list_to_set(obj:dict):
            for k, v in obj.items(): 
                if isinstance(v, list):
                    obj[k] = set(v)
            return obj


        with self.file.open(encoding="utf-8", mode="r") as fp:
            try:
                return json.load(fp, object_hook=list_to_set)
            except json.JSONDecodeError as e:
                log.error(f'{e}')
                if self.file.with_suffix('.tmp').exists():
                    self.file.with_suffix('.tmp').rename(self.file)
                    with self.file.open(encoding="utf-8", mode="r") as fp:
                        return json.load(fp, object_hook=list_to_set)
                
                return dict() # fallback


    def write(self, data: dict) -> None:
        with self.file.with_suffix('.tmp').open(mode="w") as fp:
            json.dump(data, fp, default=list, indent=4)
            fp.flush()
            os.fsync(fp.fileno())
            
        self.file.with_suffix('.tmp').rename(self.file)
