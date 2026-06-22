"""Tests for marimba.core.distribution.base.DistributionTargetBase.

The base class is exercised transitively by s3 / dap tests; these tests
pin the contract explicitly so a future target implementation gets a
clear "did you override distribute?" signal up front.
"""

from unittest.mock import Mock

import pytest

from marimba.core.distribution.base import DistributionTargetBase
from marimba.core.utils.log import LogMixin
from marimba.core.wrappers.dataset import DatasetWrapper


class TestDistributionTargetBaseContract:
    """Cover the abstract-method enforcement and exception hierarchy."""

    @pytest.mark.unit
    def test_cannot_instantiate_abstract_base(self) -> None:
        """Direct instantiation of the ABC raises TypeError naming the missing method."""
        with pytest.raises(TypeError, match="abstract"):
            DistributionTargetBase()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_concrete_subclass_must_override_distribute(self) -> None:
        """A subclass that doesn't override `distribute` is still abstract."""

        class IncompleteTarget(DistributionTargetBase):
            pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteTarget()  # type: ignore[abstract]

    @pytest.mark.unit
    def test_concrete_subclass_with_distribute_can_instantiate(self) -> None:
        class GoodTarget(DistributionTargetBase):
            def distribute(self, dataset_wrapper: DatasetWrapper) -> None:
                return None

        # Should not raise.
        target = GoodTarget()
        assert isinstance(target, DistributionTargetBase)
        assert isinstance(target, LogMixin)

    @pytest.mark.unit
    def test_distribution_error_is_an_exception(self) -> None:
        assert issubclass(DistributionTargetBase.DistributionError, Exception)

    @pytest.mark.unit
    def test_distribution_error_message_round_trip(self) -> None:
        err = DistributionTargetBase.DistributionError("upload failed: 503")
        assert str(err) == "upload failed: 503"

    @pytest.mark.unit
    def test_subclass_can_raise_distribution_error(self) -> None:
        class FailingTarget(DistributionTargetBase):
            def distribute(self, dataset_wrapper: DatasetWrapper) -> None:
                msg = "transient: try again"
                raise DistributionTargetBase.DistributionError(msg)

        target = FailingTarget()
        with pytest.raises(DistributionTargetBase.DistributionError, match="transient"):
            target.distribute(Mock(spec=DatasetWrapper))
