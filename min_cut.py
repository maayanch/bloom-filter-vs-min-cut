import sys

from data_structure import DataStructure
from typing import List
import logging


class MinCut(DataStructure):
    """
    This class implements a min-cut data structure
    """

    def __init__(self, num_of_elements_to_save: int, num_of_buckets: int, num_of_hash_functions: int, cache_size: int):
        super().__init__(num_of_hash_functions, cache_size)
        self.logger = logging.getLogger()
        self.num_of_buckets = num_of_buckets
        self.num_of_hash_functions = num_of_hash_functions
        self.num_of_elements_to_save: int = num_of_elements_to_save
        self.table: List = []
        self.elements: set = set()
        self._init_table(num_of_rows=num_of_hash_functions, num_of_cols=num_of_buckets)

    def add_new(self, element: str) -> None:
        """
        Add a new element to the Min-Cut data structure
        :param element: The element to add
        """
        self.cache.put(element)
        self.elements.add(element)
        hashed_values: List[int] = self._get_hash_values(element, self.num_of_buckets)
        self._inc(hashed_values)

    def should_exists(self, element: str) -> bool:
        """
        :param element: The element to query
        :return: True iff the sum of the counters in the table is bigger than m/k,
                 when m is the number of elements we saw until now,
                 and k is the maximum number of elements our data structure can hold.
        """
        m: float = len(self.elements) / self.num_of_elements_to_save
        hashed_values: List[int] = self._get_hash_values(element, self.num_of_buckets)
        total_sum: int = self._count(hashed_values)
        return total_sum >= m

    def _init_table(self, num_of_rows: int, num_of_cols: int) -> None:
        """
        Init the actual data structure's table with all counters equal to 0
        :param num_of_rows: Number of rows
        :param num_of_cols: Number of columns
        """
        for i in range(num_of_rows):
            new_col: list = []
            for j in range(num_of_cols):
                new_col.append(0)
            self.table.append(new_col)

    def _count(self, cells: List[int]) -> int:
        """
        Sum the counters of the given element
        :param cells: List of tuples representing a single cell in the table
        :return: The total sum
        """
        min_val: int = sys.maxsize
        for i, cell in enumerate(cells):
            val = self.table[i][cell]
            if val < min_val:
                min_val = val
        return min_val

    def _inc(self, cells: List[int]) -> None:
        """
        Increment the counters of the given element
        :param cells: List of tuples representing a single cell in the table
        """
        for i, cell in enumerate(cells):
            self.table[i][cell] += 1
