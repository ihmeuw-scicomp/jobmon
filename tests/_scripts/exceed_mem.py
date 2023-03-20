import math
import time

import numpy as np


def exceed_mem():
    """This test creates a 2d array that is intended to use about 700MB of
    memory so that we can test that it exceeds memory constraints on the fair
    cluster"""
    n_bytes = 700_000_000
    itemsize = 8
    elements = n_bytes / itemsize
    dimensions = int(math.sqrt(elements)) + 1

    arr = np.zeros((dimensions, dimensions))
    for _ in range(0, 10):
        arr = np.concatenate((arr, arr))
    foo = arr + arr
    print(foo)
    time.sleep(30)


exceed_mem()
