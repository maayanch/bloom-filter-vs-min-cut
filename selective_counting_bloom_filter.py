from data_structure import DataStructure
import logging
import math

ALPHA = 100


class SCBloomFilter(DataStructure):
    """
    This class implements a selective counting bloom filter
    """

    def __init__(self, num_of_hash_functions: int, cache_size: int, bf_size: int, max_n: int, memory_size):
        super().__init__(num_of_hash_functions, cache_size)
        self.logger = logging.getLogger()
        self.n = 0  # number of inserted elements to the bloom filter
        self.max_n = max_n
        self._update_bound()
        self.bf_size = bf_size
        self.bloom_filter = [0] * bf_size
        self.memory_size = memory_size
        self.elements = set()

    def add_new(self, element: str) -> None:
        """
        If the a priori probability of an element x satisfies the Bloom Filter paradox (bigger than self.bound),
        we will not take the answer of the bloom filter into account after the query.
        Therefore, it is better not to even insert it in the bloom filter, so as to reduce the load of the bloom filter.
        :param element: The element to add
        """
        self.elements.add(element)
        self.cache.put(element)
        if not self._satisfy_paradox():
            if self.max_n == self.n:
                return
            self._add_element_to_bloom_filter(element)
            self.n += 1
            self._update_bound()

    def should_exists(self, element: str) -> bool:
        """
        Check if the given element exists in the Bloom Filter
        If the a priori probability of an element x satisfies the Bloom paradox, we do not want to take the answer of
        the Bloom filter into account, and therefore it is better to not even query it
        :param element: The element to query
        :return: True iff the element is in the cache by the ans of the bloom filter
        """
        bf_ans = self._check_bloom_filter(element)
        if bf_ans == 0:
            return False
        apriori_prob = self._get_apriori_prob()
        nominator = math.pow(self.bf_size, self.num_of_hash_functions) * bf_ans * apriori_prob
        denominator = nominator + math.pow(self.n * self.num_of_hash_functions, self.num_of_hash_functions) * (
                1 - apriori_prob)
        res = nominator / denominator
        return res >= (1 / (1 + ALPHA))

    def _satisfy_paradox(self):
        """
        Checks if adding an element to the Bloom Filter may satisfy the paradox
        :return:
        """
        apriori_prob = self._get_apriori_prob()
        if apriori_prob < self.bound:
            return True
        return False

    def _get_apriori_prob(self):
        return len(self.cache) / self.memory_size

    def _add_element_to_bloom_filter(self, element):
        """
        This function add an element to our bloom filter using K hash function
        :param element: Element to add
        :return:
        """
        hashed_values = self._get_hash_values(element, self.bf_size)
        for value in hashed_values:
            self.bloom_filter[value] += 1

    def _update_bound(self):
        """
        Update the bound corresponding to the sizes in each case.
        :return:
        """
        if self.n == 0:
            self.bound = 0
        else:
            self.bound = 1 / (1 + ALPHA * math.pow(2, (math.log(2, math.e) * (self.bf_size / self.max_n))))

    def _check_bloom_filter(self, element):
        """
        This function get the relevant hash value according to the element and return the multiplication of all the
        counters.
        :param element: element to check if exist
        :return: multiplication of all the counters
        """
        hashed_values = self._get_hash_values(element, self.bf_size)
        mul_res = 1
        for hash_val in hashed_values:
            if self.bloom_filter[hash_val] == 0:
                return 0
            mul_res *= self.bloom_filter[hash_val]
        return mul_res
