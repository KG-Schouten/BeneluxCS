from asynciolimiter import StrictLimiter
import collections
import time

class RateLimitException(Exception):
    pass

class RateMonitor:
    def __init__(self, window_seconds: int):
        self.window = window_seconds
        self.request_timestamps = collections.deque()

    def register_request(self):
        now = time.time()
        self.request_timestamps.append(now)
        while self.request_timestamps and self.request_timestamps[0] < now - self.window:
            self.request_timestamps.popleft()

    def current_rate(self) -> float:
        return len(self.request_timestamps) / self.window

    def debug_log(self):
        print(f"[RateMonitor] Current request rate: {self.current_rate():.2f} req/s")

# Shared rate limiter for all wrapper files
rate_limiter = StrictLimiter(400, 10)  # 400 requests per 10 seconds
rate_monitor = RateMonitor(window_seconds=1)