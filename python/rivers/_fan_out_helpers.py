"""Internal helpers for dynamic fan-out streaming collect."""


def _make_stream_generator(completion_queue, results_map):
    """Create a generator that yields results in completion order.

    Args:
        completion_queue: PyCompletionQueue instance (iterable of mapping keys)
        results_map: dict mapping key → result value
    """
    for key in completion_queue:
        yield results_map[key]


def _make_ordered_stream_generator(completion_queue, results_map):
    """Create a generator that yields results in mapping key order.

    Buffers out-of-order completions and yields when the next
    expected index is available.

    Args:
        completion_queue: PyCompletionQueue instance (iterable of mapping keys)
        results_map: dict mapping key → result value
    """
    expected_idx = 0
    buffer = {}
    for key in completion_queue:
        buffer[key] = results_map[key]
        while str(expected_idx) in buffer:
            yield buffer.pop(str(expected_idx))
            expected_idx += 1
    # Flush any remaining buffered results
    while str(expected_idx) in buffer:
        yield buffer.pop(str(expected_idx))
        expected_idx += 1
