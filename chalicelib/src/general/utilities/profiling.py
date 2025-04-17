from contextlib import contextmanager
from typing import Callable, Any
import cProfile
import pstats
import os
import sys

@contextmanager
def suppress_console_output():
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = devnull
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


def call_and_write_stats(output_stats_file_name: str, callback: Callable, *args, **kwargs) -> Any:
    '''calls the callback function, passing all following and keyword arguments, profiles the performance and writes the stats to the 
    file name specified by output_stats_file_name'''
    profiler = cProfile.Profile()
    args_repr = [repr(a) for a in args]  # Use repr() for the args to get string representations
    kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # Use !r to get string representations for kwargs
    arg_list = args_repr + kwargs_repr
    func_call_str = f"{callback.__name__}({', '.join(arg_list)})"
    
    print(f"profiling {func_call_str}...", end="")
    profiler.enable()
    with suppress_console_output():
        callback_output = callback(*args, **kwargs)
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats('time')  # Create Stats object and sort the results by time

    # To print stats to a file
    with open(f"{output_stats_file_name}.txt", "w") as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats('time')
        stats.print_stats()

    print(" Done.")

    return callback_output
