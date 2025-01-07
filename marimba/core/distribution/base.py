"""
Marimba Abstract Base Class for Distribution Targets.

This module provides an abstract base class that defines the interface for all distribution targets of Marimba datasets.

Imports:
    - ABC: Helper class that provides a standard way to create an abstract class.
    - abstractmethod: A decorator indicating abstract methods.
    - LogMixin: Mixin class that provides logging functionality.
    - DatasetWrapper: Wrapper class for Marimba datasets.

Classes:
    - DistributionTargetBase: Abstract base class that defines the interface for all distribution targets.
        - DistributionError: Base class for all distribution errors.
"""

from abc import ABC, abstractmethod

from marimba.core.utils.log import LogMixin
from marimba.core.wrappers.dataset import DatasetWrapper


class DistributionTargetBase(ABC, LogMixin):
    """
    Marimba distribution target base class. Defines the interface for all distribution targets of Marimba datasets.
    """

    class DistributionError(Exception):
        """
        Base class for all distribution errors.
        """

    @abstractmethod
    def distribute(self, dataset_wrapper: DatasetWrapper) -> None:
        """
        Distribute the given dataset to this target.

        Args:
            dataset_wrapper: The dataset to distribute.

        Raises:
            DistributionError: If the distribution fails.
        """
        raise NotImplementedError
