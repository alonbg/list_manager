from time import time
import json
from collections import Counter
from enum import IntEnum
import asyncio
import aiofiles
from humanfriendly import format_timespan
from .. import log, JsonFile


class ResolverSet(IntEnum):
    resolvable = 0
    unresolvable = 1
    none = 2
    nameServerError = 3
    timeout = 4
    dnsError = 5
    error = 6

    def __str__(self):
        return self.name
    
class AsyncJsonFileWriter(JsonFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def write(self, data: dict) -> None:
        dump = json.dumps(data, default=list, indent=4)
        async with aiofiles.open(self.file.with_suffix('.tmp'), 'w') as afp:
            await afp.write(dump)
            await afp.flush()
        
        self.file.with_suffix('.tmp').rename(self.file)


class RuntimeEstimator:
    def __init__(self, start_time=time(), total_items:int=0):
        self.start_time=start_time
        self.total_items = total_items
        self.counters = Counter()

    def update(self, idx, count):
        self.counters[idx] += count

    def is_done(self) -> bool:
        return sum(self.counters) == self.total_items

    def estimate(self):
            processed = self.counters.total()
            remaining = self.total_items - processed
            elapsed_time = time() - self.start_time
            estimated_total_time = (self.total_items * elapsed_time) / processed if processed > 0 else 0
            estimated_remaining_time = estimated_total_time - elapsed_time
            return [remaining, estimated_remaining_time, processed, estimated_total_time]

    def log(self):
        estimate =  self.estimate()
        log.info(
            f"Remaining: {estimate[0]}, estimated time to finish: {format_timespan(estimate[1])}")

    async def loop(self, completion, sleep):
        while True:
            self.log()
            await asyncio.sleep(sleep)
            if completion.is_set():
                break

                 

