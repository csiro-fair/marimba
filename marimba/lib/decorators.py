"""
Multithreaded decorators and utilities.

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

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, Callable, Iterable, TypeVar

# Define a generic type variable
T = TypeVar("T", bound=Callable[..., Any])


def multithreaded(logger: logging.Logger) -> Callable[[T], T]:
    """
    Multithreaded method decorator.

    Args:
        logger: Logging object to record any errors encountered while processing.

    Returns:
        The decorated function.
    """

    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, items: Iterable[Any], **kwargs: Any) -> None:
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(func, *args, item=item, **kwargs): item for item in items}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error processing {item}: {e}")

        return wrapper  # type: ignore

    return decorator
