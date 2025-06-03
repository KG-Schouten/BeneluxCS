import asyncio
import time
from collections import deque
from data_processing.faceit_api.logging_config import api_logger

request_limit = 350  # Maximum number of calls allowed in the time window
interval = 10  # Time window in seconds
concurrency = 7 # Number of concurrent requests

class RateLimitException(Exception):
    """Custom exception for rate limit errors."""
    pass

class SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.call_times = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                window_start = now - self.period

                # Clear outdated calls
                while self.call_times and self.call_times[0] < window_start:
                    self.call_times.popleft()

                # Log current count
                api_logger.info(f"Current request count in window: {len(self.call_times)}/{self.max_calls}")
                
                if len(self.call_times) < self.max_calls:
                    self.call_times.append(now)
                    return # Success: allowed in
                
                else:
                    sleep_time = self.call_times[0] + self.period - now
                    sleep_time = max(sleep_time, 0.01) # Avoid zero/negative sleep time
                    # logger.info(f"Rate limit hit. Sleeping for {sleep_time:.2f}s")

            await asyncio.sleep(sleep_time)

class RequestDispatcher:
    def __init__(self, request_limit: int=300, interval: int=10, concurrency: int=30):
        self.queue = asyncio.Queue()
        self.workers = []
        self.concurrency = concurrency
        self._running = False
        self.rate_limiter = SlidingWindowRateLimiter(request_limit, interval)

    async def start(self):
        if self._running:
            return
        self._running = True
        for _ in range(self.concurrency):
            worker = asyncio.create_task(self._worker())
            self.workers.append(worker)

    async def _worker(self):
        while True:
            func, args, kwargs, fut = await self.queue.get()
            try:
                await self.rate_limiter.acquire()
                result = await func(*args, **kwargs)
                fut.set_result(result)
            except Exception as e:
                fut.set_exception(e)
            finally:
                self.queue.task_done()

    async def run(self, func, *args, **kwargs):
        fut = asyncio.get_running_loop().create_future()
        await self.queue.put((func, args, kwargs, fut))
        return await fut

    async def stop(self):
        for w in self.workers:
            w.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers = []
        self._running = False # <--- Reset this so start() can recreate workers
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()