from abc import ABC, abstractmethod
import asyncio

class SingletonInst(ABC):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonInst, cls).__new__(cls)
            
        else:
            raise Exception(f"{cls.__name__} is a singleton")
        return cls._instance


class SemaphoreDecorator:
    @staticmethod
    def wrap(semaphore):
        def decorator(func):
            async def wrapper(*args, **kwargs):
                async with semaphore:
                    return await func(*args, **kwargs)
            return wrapper
        return decorator


class AsyncBatchWriter(ABC, SemaphoreDecorator):
    def __init__(self):
        self.write_batch = self.wrap(asyncio.Semaphore(1))(self.write_batch)

    @abstractmethod
    async def write_batch(self, batch, **kwargs):
        pass

class AsyncBatchProcessor(ABC, SemaphoreDecorator):
    def __init__(self, max_concurrent_tasks=5, batch_size=10):
        self.batch_size = batch_size     
        self.process = self.wrap(asyncio.Semaphore(max_concurrent_tasks))(self.process)
        
    @abstractmethod
    async def process(self, item):
        pass

    # process items in smaller batches
    async def process_batch(self, items):
        self.batch_size = min(self.batch_size, len(items))
        for i in range(0, len(items), self.batch_size):
            yield await asyncio.gather(*(self.process(item) for item in items[i:i + self.batch_size]))
