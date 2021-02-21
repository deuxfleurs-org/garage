import hashlib
import bisect
import xxhash

def hash_str(s):
    xxh = xxhash.xxh64()
    xxh.update(s.encode('ascii'))
    return xxh.hexdigest()

def sha256_str(s):
    return hashlib.sha256(s.encode('ascii')).hexdigest()

def walk_ring_from_pos(tokens, dcs, start, rep):
    ret = []
    ret_dcs = set()
    delta = 0
    while len(ret) < rep:
        i = (start + delta) % len(tokens)
        delta = delta + 1

        (token_k, token_dc, token_node) = tokens[i]
        if token_dc not in ret_dcs:
            ret_dcs |= set([token_dc])
            ret.append(token_node)
        elif len(ret_dcs) == len(dcs) and token_node not in ret:
            ret.append(token_node)
    return ret

def count_tokens_per_node(tokens):
    tokens_of_node = {}
    for _, _, token_node in tokens:
        if token_node not in tokens_of_node:
            tokens_of_node[token_node] = 0
        tokens_of_node[token_node] += 1
    print("#tokens per node:")
    for node, ntok in sorted(list(tokens_of_node.items())):
        print(node, ": ", ntok)


def method1(nodes):
    tokens = []
    dcs = set()
    for (node_id, dc, n_tokens) in nodes:
        dcs |= set([dc])
        for i in range(n_tokens):
            token = hash_str(f"{node_id} {i}")
            tokens.append((token, dc, node_id))
    tokens.sort(key=lambda tok: tok[0])

    #print(tokens)
    count_tokens_per_node(tokens)

    space_of_node = {}

    def walk_ring(v, rep):
        i = bisect.bisect_left([tok for tok, _, _ in tokens], hash_str(v))
        return walk_ring_from_pos(tokens, dcs, i, rep)

    return walk_ring


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
    count_tokens_per_node(partition_nodes)

    dcs = list(set(node_dc for _, node_dc, _ in nodes))

        
    def walk_ring(v, rep):
        # xxh = xxhash.xxh32()
        # xxh.update(v.encode('ascii'))
        # vh = xxh.intdigest()
        # i = vh % (2**partition_bits)
        vh = hashlib.sha256(v.encode('ascii')).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        return walk_ring_from_pos(partition_nodes, dcs, i, rep)

    return walk_ring


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

    count_tokens_per_node(partitions)
    dcs = list(set(node_dc for _, node_dc, _ in nodes))

    def walk_ring(v, rep):
        vh = hashlib.sha256(v.encode('ascii')).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        return walk_ring_from_pos(partitions, dcs, i, rep)

    return walk_ring


def evaluate_method(walk_ring):
    node_data_counts = {}
    for i in range(100000):
        nodes = walk_ring(f"{i}", 3)
        for n in nodes:
            if n not in node_data_counts:
                node_data_counts[n] = 0
            node_data_counts[n] += 1
    print("Number of data items per node:")
    for n, v in sorted(list(node_data_counts.items())):
        print(n, ": ", v)


if __name__ == "__main__":
    print("------")
    print("method 1 (standard ring)")
    nodes = [('digitale', 'atuin', 64),
             ('drosera', 'atuin', 64),
             ('datura', 'atuin', 64),
             ('io', 'jupiter', 128)]
    method1_walk_ring = method1(nodes)
    evaluate_method(method1_walk_ring)

    print("------")
    print("method 2 (custom ring)")
    nodes = [('digitale', 'atuin', 10),
             ('drosera', 'atuin', 10),
             ('datura', 'atuin', 10),
             ('io', 'jupiter', 20)]
    method2_walk_ring = method2(nodes)
    evaluate_method(method2_walk_ring)

    print("------")
    print("method 3 (maglev)")
    nodes = [('digitale', 'atuin', 4),
             ('drosera', 'atuin', 4),
             ('datura', 'atuin', 4),
             ('io', 'jupiter', 8),
             #('mini', 'grog', 2),
             #('mixi', 'grog', 2),
             #('moxi', 'grog', 2),
             #('modi', 'grog', 2),
             ]
    method3_walk_ring = method3(nodes)
    evaluate_method(method3_walk_ring)
