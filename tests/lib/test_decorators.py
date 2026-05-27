"""Tests for marimba.lib.decorators module."""

import logging
from typing import Any

import pytest
from pytest_mock import MockerFixture

from marimba.lib.decorators import multithreaded


class TestMultithreadedDecorator:
    """Test multithreaded decorator functionality."""

    @pytest.fixture
    def test_target_class(self) -> Any:
        """Create a simple target class for testing decorator application."""

        class TestTarget:
            """Simple test class for decorator testing."""

        return TestTarget()

    @pytest.fixture
    def test_items(self) -> list[int]:
        """Create test items for multithreaded processing."""
        return [1, 2, 3, 4, 5]

    @pytest.fixture
    def mock_logger(self, mocker: MockerFixture) -> Any:
        """Create a mock logger for testing."""
        return mocker.Mock(spec=logging.Logger)

    @pytest.mark.unit
    def test_decorator_import_and_structure(self) -> None:
        """Test that multithreaded decorator can be imported and has correct structure.

        This unit test verifies that the decorator is properly importable and callable,
        ensuring basic module structure and interface requirements are met.
        """
        # Arrange & Act - decorator import handled by module import

        # Assert
        assert multithreaded is not None, "Decorator should be importable from module"
        assert callable(multithreaded), "Decorator should be a callable function"

        # Test that decorator function returns a decorator when called
        decorator_instance = multithreaded()
        assert callable(decorator_instance), "Decorator instance should be callable for function decoration"

        # Test that decorator accepts max_workers parameter
        decorator_with_workers = multithreaded(max_workers=2)
        assert callable(decorator_with_workers), "Decorator should accept max_workers parameter"

    @pytest.mark.unit
    def test_decorated_function_basic_behavior(self, test_target_class: Any, mock_logger: Any) -> None:
        """Test that the multithreaded decorator creates functional decorated methods.

        This unit test verifies that the decorator correctly transforms a function
        to accept the expected parameters (items, logger) and produces results for
        basic single-item processing scenarios.
        """

        # Arrange
        @multithreaded()
        def sample_method(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Sample method for testing decoration."""
            return f"processed_{item}_{thread_num}"

        test_items = [42]

        # Act
        results = sample_method(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert callable(sample_method), "Decorated method should remain callable"
        assert isinstance(results, list), "Decorated method should return a list"
        assert len(results) == 1, f"Should return 1 result for 1 item, got {len(results)}"
        assert results[0] == "processed_42_0", f"Should return 'processed_42_0' for single item, got {results[0]}"

    @pytest.mark.unit
    def test_multithreaded_successful_processing(
        self,
        test_target_class: Any,
        test_items: list[int],
        mock_logger: Any,
    ) -> None:
        """Test multithreaded decorator with successful processing of all items.

        This unit test verifies that the decorator correctly processes multiple items
        concurrently, calls the underlying function for each item with proper thread numbering,
        and returns results in the expected format. Uses mocked logger to isolate the decorator logic.
        """

        # Arrange
        @multithreaded(max_workers=2)
        def process_item(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item with thread information."""
            return f"result_{item}_{thread_num}"

        # Act
        results = process_item(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert isinstance(results, list), "Function should return a list of results"
        assert len(results) == len(
            test_items,
        ), f"Should return {len(test_items)} results for {len(test_items)} input items, got {len(results)}"

        # Verify all items were processed (order may vary due to threading)
        processed_items = set()
        for result in results:
            assert isinstance(result, str), "Each result should be a string"
            assert result.startswith("result_"), "Each result should have expected format"
            # Extract item number from result
            parts = result.split("_")
            assert len(parts) >= 3, f"Result should have format 'result_<item>_<thread>', got {result}"
            item_num = int(parts[1])
            processed_items.add(item_num)

            # Verify thread numbering format (thread_num is zero-padded index, not thread pool worker ID)
            thread_num = parts[2]
            assert thread_num.isdigit(), f"Thread number should be numeric, got {thread_num}"
            thread_idx = int(thread_num)
            assert (
                0 <= thread_idx < len(test_items)
            ), f"Thread index should be in valid range [0, {len(test_items)-1}], got {thread_idx}"

        # Verify all input items were processed exactly once
        assert processed_items == set(
            test_items,
        ), f"All input items should be processed, got {processed_items} vs expected {set(test_items)}"

    @pytest.mark.integration
    def test_multithreaded_concurrent_execution(
        self,
        test_target_class: Any,
        mock_logger: Any,
        mocker: MockerFixture,
    ) -> None:
        """Test multithreaded decorator creates and uses ThreadPoolExecutor properly.

        This integration test verifies that the decorator correctly instantiates
        ThreadPoolExecutor with the specified max_workers parameter and submits
        tasks for concurrent execution. It mocks the ThreadPoolExecutor to verify
        the correct integration between the decorator and the concurrent.futures module.
        """
        # Arrange
        mock_executor_instance = mocker.Mock()
        mock_executor_instance.__enter__ = mocker.Mock(return_value=mock_executor_instance)
        mock_executor_instance.__exit__ = mocker.Mock(return_value=None)

        # Create mock futures for the submitted tasks
        mock_futures = []
        expected_items = [1, 2, 3, 4, 5]

        for i, item in enumerate(expected_items):
            mock_future = mocker.Mock()
            mock_future.result.return_value = f"processed_{item}_{i:01d}"
            mock_futures.append(mock_future)

        mock_executor_instance.submit.side_effect = mock_futures

        # Mock ThreadPoolExecutor class to return our mock instance
        mock_executor_class = mocker.patch(
            "marimba.lib.decorators.ThreadPoolExecutor",
            return_value=mock_executor_instance,
        )

        # Mock as_completed to return futures in order for predictable testing
        mocker.patch(
            "marimba.lib.decorators.as_completed",
            return_value=mock_futures,
        )

        @multithreaded(max_workers=3)
        def process_item_concurrently(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process item in multithreaded context."""
            return f"processed_{item}_{thread_num}"

        # Act
        results = process_item_concurrently(test_target_class, items=expected_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        # Verify ThreadPoolExecutor was created with correct max_workers
        mock_executor_class.assert_called_once_with(max_workers=3)

        # Verify context manager was used properly
        mock_executor_instance.__enter__.assert_called_once()
        mock_executor_instance.__exit__.assert_called_once()

        # Verify submit was called for each item with correct parameters
        assert mock_executor_instance.submit.call_count == len(expected_items), (
            f"Should submit {len(expected_items)} tasks to executor, "
            f"got {mock_executor_instance.submit.call_count} submissions"
        )

        # Verify all items were processed and results returned
        assert len(results) == len(expected_items), f"Should return {len(expected_items)} results, got {len(results)}"

        # Verify result format and content
        assert isinstance(results, list), "Should return a list of results"
        assert all(isinstance(result, str) for result in results), "All results should be strings"

        # Verify all expected items were processed (order may vary due to mocking)
        result_items = set()
        for result in results:
            # Extract item number from format: processed_{item}_{thread_num}
            parts = result.split("_")
            assert len(parts) >= 3, f"Result should have format 'processed_<item>_<thread>', got {result}"
            item_num = int(parts[1])
            result_items.add(item_num)

        assert result_items == set(
            expected_items,
        ), f"Should process all expected items. Expected: {set(expected_items)}, Got: {result_items}"

    @pytest.mark.integration
    def test_multithreaded_default_reraises_first_worker_exception(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """Default behaviour is fail-fast: worker exceptions are logged and the first is re-raised."""
        test_items = [1, 2, 3, 4, 5]

        @multithreaded()
        def process_item_with_errors(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,  # noqa: ARG001
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            if item % 2 == 0:
                msg = f"Processing failed for item {item}"
                raise ValueError(msg)
            return f"success_{item}"

        with pytest.raises(ValueError, match="Processing failed for item"):
            process_item_with_errors(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Every failing worker still logs before the re-raise.
        assert mock_logger.exception.call_count >= 1, (
            "Expected at least one worker exception to be logged before the re-raise; "
            f"got {mock_logger.exception.call_count}"
        )

    @pytest.mark.integration
    def test_multithreaded_allow_partial_returns_successful_results(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """allow_partial=True opts back into the previous swallow-and-continue semantics."""
        test_items = [1, 2, 3, 4, 5]
        expected_successful_items = {1, 3, 5}
        expected_failed_items = {2, 4}

        @multithreaded(allow_partial=True)
        def process_item_with_errors(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            if item % 2 == 0:
                msg = f"Processing failed for item {item}"
                raise ValueError(msg)
            return f"success_{item}_{thread_num}"

        results = process_item_with_errors(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        assert isinstance(results, list)
        assert len(results) == len(expected_successful_items)

        processed_items = {int(r.split("_")[1]) for r in results}
        assert processed_items == expected_successful_items

        assert mock_logger.exception.call_count == len(expected_failed_items)

    @pytest.mark.unit
    def test_multithreaded_invalid_items_type_error(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """Test multithreaded decorator raises TypeError for non-Sized iterable items.

        This unit test verifies that the decorator properly validates input and raises
        TypeError when items parameter is not a Sized iterable (such as a generator),
        ensuring proper error handling and user feedback for invalid usage patterns.
        The test specifically validates that generators are rejected since they don't
        support len() operations required for thread numbering calculations.
        """

        # Arrange
        @multithreaded()
        def process_item(
            self: Any,  # noqa: ARG001
            item: Any,
            thread_num: str,  # noqa: ARG001
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item."""
            return f"result_{item}"

        # Create a non-Sized iterable (generator) - this lacks len() support
        non_sized_items = (x for x in range(3))
        expected_error_pattern = r"^items must be a Sized iterable$"

        # Act & Assert
        with pytest.raises(TypeError, match=expected_error_pattern):
            process_item(test_target_class, items=non_sized_items, logger=mock_logger)  # type: ignore[call-arg]

    @pytest.mark.unit
    def test_multithreaded_empty_items_list(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """Test multithreaded decorator with empty items list.

        This unit test verifies that the decorator correctly handles the edge case of empty
        input lists without errors, returns an empty result list, and doesn't attempt to
        process any items or create unnecessary threads. This tests the decorator's
        input validation and early return behavior for empty collections.
        """

        # Arrange
        @multithreaded()
        def process_item(
            self: Any,  # noqa: ARG001
            item: Any,
            thread_num: str,  # noqa: ARG001
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item."""
            return f"result_{item}"

        empty_items: list[int] = []

        # Act
        results = process_item(test_target_class, items=empty_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert isinstance(results, list), "Function should return a list for empty input"
        assert results == [], f"Should return empty list for empty input, got {results}"
        assert len(results) == 0, "Result list should be empty when no items are provided"

        # Verify no processing or logging occurred for empty input
        assert mock_logger.exception.call_count == 0, "Should not log any exceptions for empty input"
        assert mock_logger.info.call_count == 0, "Should not log any info messages for empty input"

    @pytest.mark.unit
    def test_multithreaded_default_logger_usage(
        self,
        test_target_class: Any,
    ) -> None:
        """Test multithreaded decorator uses default logger when none provided.

        This unit test verifies that the decorator properly falls back to
        a default logger when no logger is explicitly provided, ensuring
        the decorator can function independently without external logger injection.
        """

        # Arrange
        @multithreaded()
        def process_item(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item."""
            return f"result_{item}_{thread_num}"

        test_items = [1, 2]

        # Act - don't provide logger parameter to test default logger fallback
        results = process_item(test_target_class, items=test_items)  # type: ignore[call-arg]

        # Assert
        assert isinstance(results, list), "Function should return a list of results when using default logger"
        assert len(results) == 2, f"Should process both items with default logger, got {len(results)} results"

        # Verify all items were processed with expected format
        processed_items = set()
        for result in results:
            assert isinstance(result, str), "Each result should be a string"
            assert result.startswith("result_"), "Results should have expected format when using default logger"
            # Extract item number to verify all items were processed
            parts = result.split("_")
            assert len(parts) >= 3, f"Result should have format 'result_<item>_<thread>', got {result}"
            item_num = int(parts[1])
            processed_items.add(item_num)

        # Verify all input items were processed exactly once
        assert processed_items == {1, 2}, f"Both items should be processed with default logger, got {processed_items}"

    @pytest.mark.unit
    def test_multithreaded_max_workers_parameter(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """Test multithreaded decorator accepts and uses max_workers parameter.

        This unit test verifies that the decorator correctly accepts the max_workers
        parameter without errors and processes all items successfully. The test
        validates parameter acceptance, basic functionality, and correct thread
        numbering format based on the number of items being processed.
        """

        # Arrange
        @multithreaded(max_workers=1)
        def process_item(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item with constrained worker thread."""
            return f"single_worker_{item}_{thread_num}"

        test_items = [1, 2, 3]

        # Act
        results = process_item(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert isinstance(results, list), "Function should return a list of results"
        assert len(results) == 3, f"Should process all 3 items with max_workers=1, got {len(results)} results"

        # Verify all items were processed with correct format
        processed_items = set()
        for result in results:
            assert result.startswith("single_worker_"), "Results should have expected prefix format"
            parts = result.split("_")
            assert len(parts) == 4, f"Result should have format 'single_worker_<item>_<thread>', got {result}"

            # Extract item number (parts[2] in format: single_worker_<item>_<thread>)
            item_num = int(parts[2])
            processed_items.add(item_num)

            # Verify thread index format (parts[3] - zero-padded sequential index)
            thread_num = parts[3]
            assert thread_num.isdigit(), f"Thread index should be numeric, got {thread_num}"
            thread_idx = int(thread_num)
            assert (
                0 <= thread_idx < len(test_items)
            ), f"Thread index should be in range [0, {len(test_items)-1}], got {thread_idx}"

        # Verify all input items were processed exactly once
        assert processed_items == {1, 2, 3}, f"All items should be processed exactly once, got {processed_items}"

    @pytest.mark.unit
    def test_multithreaded_single_item_zero_padding(
        self,
        test_target_class: Any,
        mock_logger: Any,
    ) -> None:
        """Test multithreaded decorator zero-padding logic with single item.

        This unit test verifies that the decorator correctly calculates zero-padding
        for thread indices based on the total number of items. For a single item,
        the thread index should be '0' without zero-padding since only one digit is needed.
        Tests the zero-padding formula: math.ceil(math.log10(len(items) + 1))
        """

        # Arrange
        @multithreaded()
        def process_single_item(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process a single item to test zero-padding logic."""
            return f"single_{item}_{thread_num}"

        test_items = [42]

        # Act
        results = process_single_item(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert isinstance(results, list), "Function should return a list"
        assert len(results) == 1, f"Should return 1 result for 1 item, got {len(results)}"

        result = results[0]
        assert isinstance(result, str), "Result should be a string"
        assert result == "single_42_0", f"Expected 'single_42_0' for single item, got {result}"

        # Verify thread number format for single item (should be '0' - zero-padded index)
        parts = result.split("_")
        assert len(parts) == 3, f"Result should have format 'single_<item>_<thread>', got {result}"
        thread_num = parts[2]
        assert thread_num == "0", f"Thread index should be '0' for single item (zero-padded), got '{thread_num}'"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("item_count", "expected_width"),
        [
            (1, 1),  # math.ceil(log10(1 + 1)) = math.ceil(log10(2)) = math.ceil(0.301) = 1
            (9, 1),  # math.ceil(log10(9 + 1)) = math.ceil(log10(10)) = math.ceil(1.0) = 1
            (10, 2),  # math.ceil(log10(10 + 1)) = math.ceil(log10(11)) = math.ceil(1.041) = 2
            (99, 2),  # math.ceil(log10(99 + 1)) = math.ceil(log10(100)) = math.ceil(2.0) = 2
            (100, 3),  # math.ceil(log10(100 + 1)) = math.ceil(log10(101)) = math.ceil(2.004) = 3
        ],
        ids=["single_item", "single_digit", "double_digit", "two_digit_max", "triple_digit"],
    )
    def test_multithreaded_thread_numbering_format(
        self,
        test_target_class: Any,
        mock_logger: Any,
        item_count: int,
        expected_width: int,
    ) -> None:
        """Test multithreaded decorator thread numbering format with zero-padding.

        This unit test verifies that the decorator correctly formats thread indices
        with appropriate zero-padding based on the total number of items. Thread
        numbers are sequential indices (0, 1, 2, ...), not actual thread pool IDs.
        The zero-padding width is calculated as: math.ceil(math.log10(len(items) + 1))
        """

        # Arrange
        @multithreaded()
        def process_item_with_numbering(
            self: Any,  # noqa: ARG001
            item: int,
            thread_num: str,
            logger: logging.Logger,  # noqa: ARG001
        ) -> str:
            """Process item to test thread numbering format."""
            return f"item_{item}_thread_{thread_num}"

        test_items = list(range(item_count))

        # Act
        results = process_item_with_numbering(test_target_class, items=test_items, logger=mock_logger)  # type: ignore[call-arg]

        # Assert
        assert len(results) == item_count, f"Should process {item_count} items, got {len(results)}"

        # Verify thread index formatting (zero-padded sequential indices)
        thread_indices = set()
        expected_indices = {f"{i:0{expected_width}}" for i in range(item_count)}

        for result in results:
            parts = result.split("_")
            assert len(parts) == 4, f"Result should have format 'item_<item>_thread_<thread>', got {result}"
            thread_num = parts[3]

            assert len(thread_num) == expected_width, (
                f"Thread index should have width {expected_width} for {item_count} items, "
                f"got '{thread_num}' with width {len(thread_num)}"
            )
            assert thread_num.isdigit(), f"Thread index should be numeric, got '{thread_num}'"

            thread_idx = int(thread_num)
            assert (
                0 <= thread_idx < item_count
            ), f"Thread index should be in range [0, {item_count-1}], got {thread_idx}"
            thread_indices.add(thread_num)

        # Verify we have unique thread indices for each item and they match expected format
        assert (
            len(thread_indices) == item_count
        ), f"Should have {item_count} unique thread indices for {item_count} items, got {len(thread_indices)}"
        assert thread_indices == expected_indices, (
            f"Thread indices should match expected zero-padded format. "
            f"Expected: {expected_indices}, Got: {thread_indices}"
        )
