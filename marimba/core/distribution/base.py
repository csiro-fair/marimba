"""
Marimba Abstract Base Class for Distribution Targets.

This module provides an abstract base class that defines the interface for all distribution targets of Marimba datasets.

"""

from abc import ABC, abstractmethod

from marimba.core import MarimbaError
from marimba.core.utils.log import LogMixin
from marimba.core.wrappers.dataset import DatasetWrapper


class DistributionTargetBase(ABC, LogMixin):
    """
    Marimba distribution target base class. Defines the interface for all distribution targets of Marimba datasets.
    """

    class DistributionError(MarimbaError):
        """
        Base class for all distribution errors.
        """

    @abstractmethod
    def distribute(self, dataset_wrapper: DatasetWrapper, *, dry_run: bool = False) -> None:
        """
        Distribute the given dataset to this target.

        Args:
            dataset_wrapper: The dataset to distribute.
            dry_run: When True, resolve and log everything the distribution would do (destination, file count,
                total size, per-file keys) without transferring anything.

        Raises:
            DistributionError: If the distribution fails.
        """
        raise NotImplementedError
