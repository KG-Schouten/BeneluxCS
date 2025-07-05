import asyncio
import time
import random
from collections import deque
from data_processing.faceit_api.logging_config import api_logger

request_limit = 350  # Maximum number of calls allowed in the time window
interval = 10        # Time window in seconds
concurrency = 7      # Number of concurrent requests

class RateLimitException(Exception):
    """Custom exception for rate limit errors."""
    def __init__(self, message="Rate limit exceeded. Please try again later."):
        super().__init__(message)
        self.message = message

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

                api_logger.info(
                    f"[RateLimiter] {len(self.call_times)}/{self.max_calls} in the last {self.period}s. "
                    f"Now: {now:.3f}, Window start: {window_start:.3f}"
                )

                if len(self.call_times) < self.max_calls:
                    self.call_times.append(now)
                    return
                else:
                    sleep_time = self.call_times[0] + self.period - now
                    sleep_time = max(sleep_time, 0.01)
                    api_logger.warning(
                        f"[RateLimiter] Rate limit exceeded. Sleeping for {sleep_time:.3f}s "
                        f"(Next available slot: {self.call_times[0] + self.period:.3f})"
                    )

            await asyncio.sleep(sleep_time + random.uniform(0.01, 0.1))

class RequestDispatcher:
    def __init__(self, request_limit: int = 300, interval: int = 10, concurrency: int = 30):
        self.queue = asyncio.Queue()
        self.workers = []
        self.concurrency = concurrency
        self._running = False
        self.rate_limiter = SlidingWindowRateLimiter(request_limit, interval)

    async def start(self):
        if self._running:
            return
        self._running = True
        for i in range(self.concurrency):
            worker = asyncio.create_task(self._worker(i))
            self.workers.append(worker)
        api_logger.info(f"[Dispatcher] Started {self.concurrency} workers.")

    async def _worker(self, worker_id):
        while True:
            func, args, kwargs, fut = await self.queue.get()
            request_id = kwargs.pop("request_id", f"req-{time.time():.3f}")
            queued_at = time.monotonic()
            api_logger.info(f"[Worker-{worker_id}] [{request_id}] Queued at {queued_at:.3f}. Queue size: {self.queue.qsize()}")

            try:
                await self.rate_limiter.acquire()
                started_at = time.monotonic()
                api_logger.info(f"[Worker-{worker_id}] [{request_id}] Started at {started_at:.3f}.")
                try:
                    result = await asyncio.wait_for(func(*args, **kwargs), timeout=10)
                except asyncio.TimeoutError:
                    api_logger.error(f"[Worker-{worker_id}] [{request_id}] Timed out.")
                    fut.set_exception(TimeoutError("Request timed out"))
                    continue
                finished_at = time.monotonic()
                api_logger.info(f"[Worker-{worker_id}] [{request_id}] Finished at {finished_at:.3f}. Duration: {finished_at - started_at:.3f}s")
                fut.set_result(result)
            except Exception as e:
                api_logger.exception(f"[Worker-{worker_id}] [{request_id}] Error: {e}")
                fut.set_exception(e)
            finally:
                self.queue.task_done()

    async def run(self, func, *args, request_id=None, **kwargs):
        fut = asyncio.get_running_loop().create_future()
        if request_id:
            kwargs["request_id"] = request_id
        await self.queue.put((func, args, kwargs, fut))
        api_logger.info(f"[Dispatcher] Task queued. Queue size is now: {self.queue.qsize()}")
        return await fut

    async def stop(self):
        for w in self.workers:
            w.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        api_logger.info(f"[Dispatcher] Stopped all workers.")
        self.workers = []
        self._running = False

    async def __aenter__(self) -> 'RequestDispatcher':
        if self._running:
            raise RuntimeError("Dispatcher is already running")
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            api_logger.error(f"[Dispatcher] Exception occurred: {exc_val}")
        await self.stop()