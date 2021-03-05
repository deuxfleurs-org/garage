#!/usr/bin/env python3

import hashlib
import bisect
import xxhash
import numpy as np

REPLICATION_FACTOR = 3

def hash_str(s):
    xxh = xxhash.xxh64()
    xxh.update(s.encode('ascii'))
    return xxh.hexdigest()

def sha256_str(s):
    return hashlib.sha256(s.encode('ascii')).hexdigest()

def walk_ring_from_pos(tokens, dcs, start):
    ret = []
    ret_dcs = set()
    delta = 0
    while len(ret) < REPLICATION_FACTOR:
        i = (start + delta) % len(tokens)
        delta = delta + 1

        (token_k, token_dc, token_node) = tokens[i]
        if token_dc not in ret_dcs:
            ret_dcs |= set([token_dc])
            ret.append(token_node)
        elif len(ret_dcs) == len(dcs) and token_node not in ret:
            ret.append(token_node)
    return ret

"""
def count_tokens_per_node(tokens):
    tokens_of_node = {}
    for _, _, token_node in tokens:
        if token_node not in tokens_of_node:
            tokens_of_node[token_node] = 0
        tokens_of_node[token_node] += 1
    print("#tokens per node:")
    for node, ntok in sorted(list(tokens_of_node.items())):
        print("-", node, ": ", ntok)
"""

def count_partitions_per_node(ring_node_list):
    tokens_of_node = {}
    for nodelist in ring_node_list:
        for node_id in nodelist:
            if node_id not in tokens_of_node:
                tokens_of_node[node_id] = 0
            tokens_of_node[node_id] += 1
    print("#partitions per node:")
    for node, ntok in sorted(list(tokens_of_node.items())):
        print("-", node, ": ", ntok)


def method1(nodes):
    tokens = []
    dcs = set()
    for (node_id, dc, n_tokens) in nodes:
        dcs |= set([dc])
        for i in range(n_tokens):
            token = hash_str(f"{node_id} {i}")
            tokens.append((token, dc, node_id))
    tokens.sort(key=lambda tok: tok[0])

    space_of_node = {}

    def walk_ring(v):
        i = bisect.bisect_left([tok for tok, _, _ in tokens], hash_str(v))
        return walk_ring_from_pos(tokens, dcs, i)

    ring_node_list = [walk_ring_from_pos(tokens, dcs, i) for i in range(len(tokens))]

    return walk_ring, ring_node_list


def method2(nodes):
    partition_bits = 10
    partitions = list(range(2**partition_bits))
    def partition_node(i):
        h, hn, hndc = None, None, None
        for (node_id, node_dc, n_tokens) in nodes:
            for tok in range(n_tokens):
                hnode = hash_str(f"partition {i} node {node_id} token {tok}")
                if h is None or hnode < h:
                    h = hnode
                    hn = node_id
                    hndc = node_dc
        return (i, hndc, hn)

    partition_nodes = [partition_node(i) for i in partitions]

    dcs = list(set(node_dc for _, node_dc, _ in nodes))

        
    def walk_ring(v):
        # xxh = xxhash.xxh32()
        # xxh.update(v.encode('ascii'))
        # vh = xxh.intdigest()
        # i = vh % (2**partition_bits)
        vh = hashlib.sha256(v.encode('ascii')).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        return walk_ring_from_pos(partition_nodes, dcs, i)

    ring_node_list = [walk_ring_from_pos(partition_nodes, dcs, i) for i in range(len(partition_nodes))]

    return walk_ring, ring_node_list


def method3(nodes):
    partition_bits = 10

    queues = []
    for (node_id, node_dc, n_tokens) in nodes:
        que = [(i, hash_str(f"{node_id} {i}")) for i in range(2**partition_bits)]
        que.sort(key=lambda x: x[1])
        que = [x[0] for x in que]
        queues.append((node_id, node_dc, n_tokens, que))

    partitions = [None for _ in range(2**partition_bits)]
    queues.sort(key=lambda x: hash_str(x[0]))

    # Maglev
    remaining = 2**partition_bits
    while remaining > 0:
        for toktok in range(100):
            for iq in range(len(queues)):
                node_id, node_dc, n_tokens, node_queue = queues[iq]
                if toktok >= n_tokens:
                    continue
                for qi, qv in enumerate(node_queue):
                    if partitions[qv] == None:
                        partitions[qv] = (qv, node_dc, node_id)
                        remaining -= 1
                        queues[iq] = (node_id, node_dc, n_tokens, node_queue[qi+1:])
                        break

    dcs = list(set(node_dc for _, node_dc, _ in nodes))

    def walk_ring(v):
        vh = hashlib.sha256(v.encode('ascii')).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        return walk_ring_from_pos(partitions, dcs, i)

    ring_node_list = [walk_ring_from_pos(partitions, dcs, i) for i in range(len(partitions))]

    return walk_ring, ring_node_list



def method4(nodes):
    partition_bits = 10

    partitions = [[] for _ in range(2**partition_bits)]
    dcs = list(set(node_dc for _, node_dc, _ in nodes))

    # Maglev, improved for several replicas on several DCs
    for ri in range(REPLICATION_FACTOR):
        queues = []
        for (node_id, node_dc, n_tokens) in nodes:
            que = [(i, hash_str(f"{node_id} {i}")) for i in range(2**partition_bits)]
            que.sort(key=lambda x: x[1])
            que = [x[0] for x in que]
            queues.append((node_id, node_dc, n_tokens, que))

        queues.sort(key=lambda x: hash_str("{} {}".format(ri, x[0])))

        remaining = 2**partition_bits
        while remaining > 0:
            for toktok in range(100):
                for iq in range(len(queues)):
                    node_id, node_dc, n_tokens, node_queue = queues[iq]
                    if toktok >= n_tokens:
                        continue
                    for qi, qv in enumerate(node_queue):
                        if len(partitions[qv]) != ri:
                            continue
                        p_dcs = set([x[0] for x in partitions[qv]])
                        p_nodes = [x[1] for x in partitions[qv]]
                        if node_dc not in p_dcs or (len(p_dcs) == len(dcs) and node_id not in p_nodes):
                            partitions[qv].append((node_dc, node_id))
                            remaining -= 1
                            queues[iq] = (node_id, node_dc, n_tokens, node_queue[qi+1:])
                            break

    def walk_ring(v):
        vh = hashlib.sha256(v.encode('ascii')).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        assert len(set([node_dc for node_dc, _ in partitions[i]])) == min(REPLICATION_FACTOR, len(dcs))
        return [node_id for _, node_id in partitions[i]]

    ring_node_list = [[node_id for _, node_id in p] for p in partitions]

    return walk_ring, ring_node_list

def evaluate_method(method, nodes):
    walk_ring, ring_node_list = method(nodes)
    print("Ring length:", len(ring_node_list))
    count_partitions_per_node(ring_node_list)

    print("Number of data items per node (100000 simulation):")
    node_data_counts = {}
    for i in range(100000):
        inodes = walk_ring(f"{i}")
        for n in inodes:
            if n not in node_data_counts:
                node_data_counts[n] = 0
            node_data_counts[n] += 1
    for n, v in sorted(list(node_data_counts.items())):
        print("-", n, ": ", v)

    dclist_per_ntok = {}
    for node_id, _, ntok in nodes:
        if ntok not in dclist_per_ntok:
            dclist_per_ntok[ntok] = []
        dclist_per_ntok[ntok].append(node_data_counts[node_id])
    list_normalized = []
    for ntok, dclist in dclist_per_ntok.items():
        avg = sum(dclist)/len(dclist)
        for v in dclist:
            list_normalized.append(v / avg)
    print("variance wrt. same-ntok mean:", "%.2f%%"%(np.var(list_normalized)*100))

    num_changes_sum = [0, 0, 0, 0]
    for n in nodes:
        nodes2 = [nn for nn in nodes if nn != n]
        _, ring_node_list_2 = method(nodes2)
        if len(ring_node_list_2) != len(ring_node_list):
            continue
        num_changes = [0, 0, 0, 0] 
        for (ns1, ns2) in zip(ring_node_list, ring_node_list_2):
            changes = sum(1 for x in ns1 if x not in ns2)
            num_changes[changes] += 1
        for i, v in enumerate(num_changes):
            num_changes_sum[i] += v / len(ring_node_list)
        print("removing", n[1], n[0], ":", " ".join(["%.2f%%"%(x*100/len(ring_node_list)) for x in num_changes]))
    print("1-node removal: partitions moved on 0/1/2/3 nodes: ", " ".join(["%.2f%%"%(x*100/len(nodes)) for x in num_changes_sum]))


if __name__ == "__main__":
    print("------")
    print("method 1 (standard ring)")
    nodes = [('digitale', 'atuin', 64),
             ('drosera', 'atuin', 64),
             ('datura', 'atuin', 64),
             ('io', 'jupiter', 128)]
    nodes2 = [('digitale', 'atuin', 64),
             ('drosera', 'atuin', 64),
             ('datura', 'atuin', 64),
             ('io', 'jupiter', 128),
             ('isou', 'jupiter', 64),
             ('mini', 'grog', 32),
             ('mixi', 'grog', 32),
             ('moxi', 'grog', 32),
             ('modi', 'grog', 32),
             ('geant', 'grisou', 128),
             ('gipsie', 'grisou', 128),
             ]
    evaluate_method(method1, nodes2)

    print("------")
    print("method 2 (custom ring)")
    nodes = [('digitale', 'atuin', 1),
             ('drosera', 'atuin', 1),
             ('datura', 'atuin', 1),
             ('io', 'jupiter', 2)]
    nodes2 = [('digitale', 'atuin', 2),
             ('drosera', 'atuin', 2),
             ('datura', 'atuin', 2),
             ('io', 'jupiter', 4),
             ('isou', 'jupiter', 2),
             ('mini', 'grog', 1),
             ('mixi', 'grog', 1),
             ('moxi', 'grog', 1),
             ('modi', 'grog', 1),
             ('geant', 'grisou', 4),
             ('gipsie', 'grisou', 4),
             ]
    evaluate_method(method2, nodes2)

    print("------")
    print("method 3 (maglev)")
    evaluate_method(method3, nodes2)


    print("------")
    print("method 4 (maglev, multi-dc twist)")
    evaluate_method(method4, nodes2)
