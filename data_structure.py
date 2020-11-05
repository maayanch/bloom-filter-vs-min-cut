from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Callable, List
import pymmh3 as mmh3
import logging


class DataStructure(ABC):
    """
    This class is an abstract class of a data structure with the following functionality:
    - Add a new element to the data structure
    - Check if a given element should exists in the data structure
    - Check if a given element exists in the data structure
    """

    def __init__(self, num_of_hash_functions: int, cache_size):
        """
        :param num_of_hash_functions: Number of hash function to generate
        """
        self.logger = logging.getLogger()
        self.hash_functions: List[Callable] = []
        self.num_of_hash_functions: int = num_of_hash_functions
        self.cache_size = cache_size
        self.cache = LRUCache(cache_size)
        self.cache_counter = 0

    @abstractmethod
    def add_new(self, element: str) -> None:
        """
        Add a new element to the data structure
        :param element: The element to add
        """
        raise NotImplementedError

    @abstractmethod
    def should_exists(self, element: str) -> bool:
        """
        :param element: The element to query
        :return: True if the given element should exists in the data structure, false otherwise
                 (e.g will return true if all the bits of the bloom filter are on,
                 regardless of the real status of the data structure)
        """
        raise NotImplementedError

    def is_exists(self, element: str) -> bool:
        """
        :param element: The element to query
        :return: True if the given element exists in the data structure, false otherwise
        """
        return self.cache.check_if_exists(element)

    def _get_hash_values(self, element: str, range_size: int) -> List[int]:
        """
        Map the given element to it counters using the hash functions
        :param element: The element to map
        :return: A list of cells indexes
        """
        output: List[int] = []
        for i in range(self.num_of_hash_functions):
            hashed_value = mmh3.hash(element, i) % range_size
            output.append(hashed_value)
        return output


class LRUCache:

    # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def check_if_exists(self, key: str) -> bool:
        if key in self.cache:
            return True
        return False

    def put(self, key: str, value=None) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __len__(self):
        return len(self.cache)
