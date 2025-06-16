import asyncio

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
    """ 
    Function to gather multiple coroutines with a progress bar.
        Way to use: "results = await gather_with_progress(tasks, desc='...', unit='...')"
    
    Args:
        coros (list): List of coroutines (tasks)
        desc (str): Description for the progress bar.
        unit (str): Unit of work for the progress bar.
    """
    tqdm = get_tqdm()
    total = len(coros)
    pbar = tqdm(total=total, desc=desc, unit=unit, smoothing=0)
    
    async def run(coro):
        result = await coro
        pbar.update(1)
        return result
    
    results = await asyncio.gather(*(run(coro) for coro in coros))
    pbar.close()
    return results

def run_async(coro):
    try:
        # Try to run using asyncio.run (works in scripts)
        return asyncio.run(coro)
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            # We're likely in a notebook; use nest_asyncio
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.get_event_loop().run_until_complete(coro)
        else:
            raise