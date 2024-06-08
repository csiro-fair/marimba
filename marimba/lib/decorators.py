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

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, Callable, Iterable, List, Optional, TypeVar, cast

from marimba.core.utils.log import get_logger

# Define a generic type variable
T = TypeVar("T", bound=Callable[..., Any])

logger = get_logger(__name__)


def multithreaded(max_workers: Optional[int] = None) -> Callable[[T], T]:
    """
    Multithreaded method decorator.

    Args:
        max_workers: Maximum number of worker threads to use. Defaults to None (uses ThreadPoolExecutor default).

    Returns:
        The decorated function.
    """

    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, items: Iterable[Any], **kwargs: Any) -> List[Any]:
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(func, *args, item=item, **kwargs): item for item in items}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing {item}: {e}")
            return results

        return cast(T, wrapper)

    return decorator