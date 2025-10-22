import asyncio
import sys

def get_tqdm():
    try:
        from IPython.core.getipython import get_ipython
        shell = get_ipython()
        if shell and shell.__class__.__name__ in ['ZMQInteractiveShell', 'Shell']: # Jupyter or IPython
            from tqdm.notebook import tqdm
        else:
            from tqdm import tqdm
    except (ImportError, NameError):
        # Probably a normal script or terminal
        from tqdm import tqdm  # fallback for scripts
    return tqdm

async def gather_with_progress(coros: list, desc="Processing", unit="tasks") -> list:
    tqdm = get_tqdm()

    # Decide whether to show progress
    try:
        from IPython.core.getipython import get_ipython
        shell = get_ipython()
        in_jupyter = shell and shell.__class__.__name__ in ['ZMQInteractiveShell', 'Shell']
    except Exception:
        in_jupyter = False

    # Also show bar in terminal (stdout is a TTY)
    use_pbar = in_jupyter or sys.stdout.isatty()

    total = len(coros)
    pbar = tqdm(total=total, desc=desc, unit=unit, smoothing=0, disable=not use_pbar)

    async def run(coro):
        result = await coro
        if use_pbar:
            pbar.update(1)
        return result

    results = await asyncio.gather(*(run(coro) for coro in coros))

    if use_pbar:
        pbar.close()
    return results

def run_async(coro_or_func, *args, **kwargs):
    import asyncio
    coro = None  # Ensure coro is always defined
    try:
        # If it's a function, call it with arguments to get the coroutine
        if callable(coro_or_func):
            coro = coro_or_func(*args, **kwargs)
        else:
            coro = coro_or_func
        if not asyncio.iscoroutine(coro):
            raise TypeError("Argument must be a coroutine")
        return asyncio.run(coro)
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            if not asyncio.iscoroutine(coro):
                raise TypeError("Argument must be a coroutine")
            return asyncio.get_event_loop().run_until_complete(coro)
        else:
            raise