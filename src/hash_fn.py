"""
FNV-1a hash function for deterministic IP partitioning.
Fast, non-cryptographic hash used in Hash Partitioning (PHẦN 3).
"""
import struct
import sys


def fnv1a_64(data: str) -> int:
    """FNV-1a 64-bit hash. Deterministic: same IP always → same reducer."""
    if isinstance(data, str):
        data = data.encode("utf-8")

    FNV_64_PRIME = 0x100000001b3
    FNV_64_INIT = 0xcbf29ce484222325

    hval = FNV_64_INIT
    for byte in data:
        hval ^= byte
        hval = (hval * FNV_64_PRIME) & 0xFFFFFFFFFFFFFFFF

    return hval


def partition_key(ip: str, n_reducers: int) -> int:
    """
    Determine which reducer owns this IP.
    hash(IP) % N ensures the same IP always routes to the same reducer.
    This is the core of Hash-based Partitioned Aggregation (O&V Ch.8).
    """
    return fnv1a_64(ip) % n_reducers


def get_partition_keys(ips: list[str], n_reducers: int) -> dict[int, list[str]]:
    """
    Group IPs by their partition key (reducer ID).
    Returns: {reducer_id: [list of IPs]}
    """
    partitions: dict[int, list[str]] = {i: [] for i in range(n_reducers)}
    for ip in ips:
        key = partition_key(ip, n_reducers)
        partitions[key].append(ip)
    return partitions
