import logging
import sys

from selective_counting_bloom_filter import SCBloomFilter
from data_structure import DataStructure
from typing import List, Tuple
from min_cut import MinCut
import random
import socket
import struct



# Selective Counting Bloom Filter Constants
BF_SIZE = 33280  # Num of bits

# General
NUM_OF_HASH_FUNCTIONS: int = 30
NUM_OF_GENERATED_HEAVY_HITTERS = 0.05
CACHE_SIZE: int = 5000

# Random IPs Generation Constants
NUM_OF_IPS_TO_INSERT: int = 100000
NUM_OF_IPS_TO_QUERY: int = 100000
INSERT_REPEAT_PERCENTAGE: int = 20
QUERY_REPEAT_PERCENTAGE: int = 20

# Min-Cut Constants
MC_NUM_OF_HEAVY_HITTERS: int = 350
MC_NUM_OF_BUCKETS: int = 350

BF_MEMORY_SIZE = 1600000
BF_MAX_NUMBER_OF_ELEMENTS: int = 2300



class Results(object):
    """
    This class holds the results of a single test
    """

    def __init__(self):
        self.total: int = 0
        self.fp: int = 0
        self.tp: int = 0
        self.fn: int = 0
        self.tn: int = 0

    def __repr__(self):
        return f'False positive amount: {self.fp}\n' \
               f'False positive rate: {self.fp / (self.tn + self.fp)}\n' \
               f'True positive amount: {self.tp}\n' \
               f'True positive rate: {self.tp / (self.tp + self.fn)}\n' \
               f'False negative amount: {self.fn}\n' \
               f'False negative rate: {self.fn / (self.fn + self.tp)}\n' \
               f'True negative amount: {self.tn}\n' \
               f'True negative rate: {self.tn / (self.tn + self.fp)}\n'


def generate_random_lists_of_ips(num_of_ips: int, repetitive_percent: int) -> Tuple[List[str], List[str]]:
    """
    Generate a list of IPs
    :param num_of_ips: Num of IPs to generate
    :param repetitive_percent: How much percent of the IPs should be repetitive IPs
    :return: A list of IPs
    """
    if repetitive_percent > 100 or repetitive_percent < 0:
        raise Exception("repetitive_percent must be a value between 0-100")
    insert: list = []
    query: list = []
    # Add random IPs to insert list
    repetitive_percent /= 100
    num_of_random_ips: int = int(num_of_ips * (1 - repetitive_percent))
    for _ in range(num_of_random_ips):
        random_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        insert.append(random_ip)
    # Add random IPs to query list
    for _ in range(num_of_random_ips):
        random_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        query.append(random_ip)
    # Add repetitive IPs
    num_of_repetitive_ips: int = int(num_of_ips * repetitive_percent)
    num_of_unique_repetitive_ips: int = int(num_of_repetitive_ips * NUM_OF_GENERATED_HEAVY_HITTERS)
    for _ in range(num_of_unique_repetitive_ips):
        repetitive_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        for _ in range(int(num_of_repetitive_ips / num_of_unique_repetitive_ips)):
            insert.append(repetitive_ip)
            query.append(repetitive_ip)
    random.shuffle(insert)
    random.shuffle(query)
    return insert, query


def _test(ds: DataStructure, ips_to_insert: List[str], ips_to_query: List[str]) -> Results:
    """
    Test the performance of a given data structure
    :param ds: The data structure to test
    :param ips_to_insert: List of IPs to add to the data structure
    :param ips_to_query: List of IPs to query the data structure
    :return: A results object, containing the num of FP, num of TP, num of FN, num of TN
    """
    for ip in ips_to_insert:
        ds.add_new(ip)
    results: Results = Results()
    for ip in ips_to_query:

        should_exists: bool = ds.should_exists(ip)

        if should_exists:

            if ds.is_exists(ip):
                results.tp += 1
            else:
                results.fp += 1
        else:
            if ds.is_exists(ip):
                results.fn += 1
            else:
                results.tn += 1
        results.total += 1
    return results


def test_min_cut(ips_to_insert: List[str], ips_to_query: List[str]) -> Results:
    """
    Test Min-Cut data structure
    :param ips_to_insert: List of IPs to add to the data structure
    :param ips_to_query: List of IPs to query the data structure
    :return: A results object, containing the num of FP, num of TP, num of FN, num of TN
    """
    min_cut_ds: MinCut = MinCut(MC_NUM_OF_HEAVY_HITTERS, MC_NUM_OF_BUCKETS, NUM_OF_HASH_FUNCTIONS, CACHE_SIZE)
    output: Results = _test(min_cut_ds, ips_to_insert, ips_to_query)
    return output


def test_sc_bloom_filter(ips_to_insert: List[str], ips_to_query: List[str]) -> Results:
    """
    Test "Selective Counting Bloom Filter" data structure
    :param ips_to_insert: List of IPs to add to the data structure
    :param ips_to_query: List of IPs to query the data structure
    :return: A results object, containing the num of FP, num of TP, num of FN, num of TN
    """
    bloom_filter_ds: SCBloomFilter = SCBloomFilter(NUM_OF_HASH_FUNCTIONS, CACHE_SIZE, BF_SIZE,
                                                   BF_MAX_NUMBER_OF_ELEMENTS, BF_MEMORY_SIZE)
    output: Results = _test(bloom_filter_ds, ips_to_insert, ips_to_query)
    return output


def compare_results(mc_results: Results, scbf_results: Results) -> None:
    """
    Check which data structure performed better
    and print some statistics
    :param mc_results: A results object of the Min-Cut data structure
    :param scbf_results: A results object of the Bloom Filter
    """
    print("Min-Cut Results:\n")
    print(mc_results)
    print()
    print("Selective Counting Bloom Filter Results:\n")
    print(scbf_results)


def main() -> None:
    """
    This method tests the performance of 2 data structures:
    - Selective counting bloom filter
    - Min-Cut data structure
    """
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    list_of_ips_to_insert: List[str]
    list_of_ips_to_query: List[str]
    list_of_ips_to_insert, list_of_ips_to_query = generate_random_lists_of_ips(NUM_OF_IPS_TO_INSERT,
                                                                               INSERT_REPEAT_PERCENTAGE)
    mc_results: Results = test_min_cut(list_of_ips_to_insert, list_of_ips_to_query)
    scbf_results: Results = test_sc_bloom_filter(list_of_ips_to_insert, list_of_ips_to_query)
    compare_results(mc_results, scbf_results)


if __name__ == '__main__':
    main()
