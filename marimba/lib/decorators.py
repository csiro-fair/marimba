"""
Marimba Standard Library Decorators.

This module provides a decorator for easily processing items in a multithreaded manner,
as well as supporting type definitions and common imports.

Imports:
    - logging: Logging utilities for recording errors and other events.
    - concurrent.futures.ThreadPoolExecutor: A thread pool executor for concurrent processing.
    - concurrent.futures.as_completed: A function to iterate over completed futures.
    - functools.wraps: A decorator to preserve metadata of wrapped functions.
    - typing.Any: A type hint indicating any type is accepted.
    - typing.Callable: A type hint for callable objects such as functions.
    - typing.Iterable: A type hint for objects that can be iterated over.

Functions:
    - multithreaded: A decorator to process items in a multithreaded manner.
"""

import math
from collections.abc import Callable, Iterable, Sized
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, TypeVar, cast

from marimba.core.utils.log import get_logger

# Define a generic type variable
T = TypeVar("T", bound=Callable[..., Any])

logger = get_logger(__name__)


def multithreaded(max_workers: int | None = None) -> Callable[[T], T]:
    """
    Multithreaded method decorator.

    Args:
        max_workers: Maximum number of worker threads to use. Defaults to None (uses ThreadPoolExecutor default).

    Returns:
        The decorated function.
    """

    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(self: Any, *args: Any, items: Iterable[Any], **kwargs: Any) -> list[Any]:  # noqa: ANN401
            if not isinstance(items, Sized):
                raise TypeError("items must be a Sized iterable")
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        func,
                        self,
                        *args,
                        item=item,
                        thread_num=f"{i:0{math.ceil(math.log10(len(items) + 1))}}",
                        **kwargs,
                    ): item
                    for i, item in enumerate(items)
                }
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.logger.exception(f"Error processing {item}: {e}")
            return results

        return cast(T, wrapper)

    return decorator
