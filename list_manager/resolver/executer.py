import time
import signal
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread, get_ident
import multiprocessing as mp
from .utils import RuntimeEstimator
from .. import log

class ThreadedAsyncExecuter:
    def __init__(self, min_worker_share=100, max_workers=round(mp.cpu_count()*1.7)):
        self.max_workers = max(max_workers, 2)
        self.min_worker_share = min_worker_share

    def execute(self, items, processor_factory, async_writer):
        
        if not items:
            return

        writer_loop = asyncio.new_event_loop()
        estimator = RuntimeEstimator(total_items=len(items))
        results_queue = asyncio.Queue()
        processing_completed = asyncio.Event()  # New event for completion
        
        from rich.progress import Progress
        def writer_thread_factory():
            nonlocal writer_loop, estimator, results_queue, processing_completed
            
            async def writer_wrapper():
                # progress_bar = self.get_progress_bar()
                bar = progress.add_task("[green]Resolving...", total=len(items))
               
                while not processing_completed.is_set():
                     with Progress() as progress:
                        try:
                            results = await asyncio.wait_for(results_queue.get(), timeout=1.0)
                            await async_writer(results)
                            progress.update(bar, advance=len(results))
                            estimator.log()
                        except asyncio.TimeoutError:
                            # Timeout occurred, no item in the queue
                            continue
            
            def writer_loop_wrapper():
                asyncio.set_event_loop(writer_loop)
                try:
                    writer_loop.run_until_complete(writer_wrapper())
                finally:
                    # Cancel all pending tasks
                    for task in asyncio.all_tasks(writer_loop):
                        task.cancel()
                    writer_loop.run_until_complete(writer_loop.shutdown_asyncgens())
                
            return Thread(target=writer_loop_wrapper)

        # Start the writer thread
        writer_thread = writer_thread_factory()
        writer_thread.start()
        time.sleep(0.5)

        def processor_thread_factory(segment):
            nonlocal writer_loop, estimator, results_queue
  
            async def processor_wrapper():
                ident = get_ident()
                try:
                    batch_processor = processor_factory()
                    async for batch_results in batch_processor.process_batch(segment):
                        await results_queue.put(batch_results)
                        estimator.update(ident, len(batch_results))
                        if processing_completed.is_set():
                            break  # Exit loop gracefully
                except Exception as e:
                    log.exception(f"thread {ident} error in processor_wrapper: {e}")

            processor_loop = asyncio.new_event_loop()
            def loop_wrapper():
                ident = get_ident()
                asyncio.set_event_loop(processor_loop)
                try:
                    log.info(f"thread {ident} starting processor loop")
                    processor_loop.run_until_complete(processor_wrapper())
                except Exception as e:
                    log.exception(f"thread {ident} error in loop_wrapper: {e}")
                finally:
                    # Cancel all pending tasks
                    log.info(f"thread {ident} closing processor loop")
                    for task in asyncio.all_tasks(processor_loop):
                        task.cancel()
                    processor_loop.run_until_complete(processor_loop.shutdown_asyncgens())
                    processor_loop.close()
                    log.info(f"thread {ident} processor loop closed")

            return loop_wrapper
        
        # not less than min_worker_share items per thread.
        # if needed max threads shall be reduced accordingly
        max_workers = max(1, min(len(items)/self.min_worker_share, self.max_workers))
        workers = ThreadPoolExecutor(max_workers=max_workers)
        shutdown_lock = Lock()
         
        def terminate(from_signal=False):
            nonlocal workers, writer_loop
            
            with shutdown_lock:
                if processing_completed.is_set():
                   return
                
            writer_loop.call_soon_threadsafe(processing_completed.set)
            log.info("writer & processor loops signaled to shutdown")
            
            if from_signal:
                log.info("waiting for thread pool to finish")       
                workers.shutdown(wait=True)
                log.info("thread pool shutdown")

            writer_thread.join()
            log.info("writer thread shutdown")
            
            writer_loop.close()
            log.info("writer loop shutdown")

        # Register signal handlers
        def handler(__signal, __frame):
            terminate(from_signal=True)
                
        signals_to_handle = [signal.SIGINT, signal.SIGTERM]
        for sig in signals_to_handle:
            signal.signal(sig, handler)

        # Divide items into max_workers segments
        log.info(f'workers: {max_workers}, items: {len(items)}')
        # with workers as ex:
        acc = 0
        for segment in [items[i::max_workers] for i in range(max_workers)]:
            processor_thread = processor_thread_factory(segment=segment)
            workers.submit(processor_thread)
            seg_len = len(segment)
            acc += seg_len
            log.info(f"thread items: {seg_len}, accumulated: {acc}")
        
        workers.shutdown(wait=True)
        terminate()