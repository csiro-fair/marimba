from abc import ABC, abstractmethod

from marimba.utils.log import LogMixin
from marimba.wrappers.dataset import DatasetWrapper


class DistributionTargetBase(ABC, LogMixin):
    """
    Marimba distribution target base class. Defines the interface for all distribution targets of Marimba datasets.
    """

    class DistributionError(Exception):
        """
        Base class for all distribution errors.
        """

        pass

    @abstractmethod
    def distribute(self, dataset_wrapper: DatasetWrapper):
        """
        Distribute the given dataset to this target.

        Args:
            dataset_wrapper: The dataset to distribute.

        Raises:
            DistributionError: If the distribution fails.
        """
        raise NotImplementedError
