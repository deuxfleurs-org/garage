import hashlib
import bisect
import xxhash


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
            token = hashlib.sha256(f"{node_id} {i}".encode('ascii')).hexdigest()
            tokens.append((token, dc, node_id))
    tokens.sort(key=lambda tok: tok[0])

    count_tokens_per_node(tokens)

    space_of_node = {}

    def walk_ring(v, rep):
        i = bisect.bisect_left([tok for tok, _, _ in tokens], hashlib.sha256(v).hexdigest())
        return walk_ring_from_pos(tokens, dcs, i, rep)

    return walk_ring


def method2(nodes):
    partition_bits = 10
    partitions = list(range(2**partition_bits))
    def partition_node(i):
        h, hn, hndc = None, None, None
        for (node_id, node_dc, n_tokens) in nodes:
            for tok in range(n_tokens):
                #hnode = hashlib.sha256(f"{i} {node_id} {tok}".encode('ascii')).hexdigest()
                xxh = xxhash.xxh64()
                xxh.update(f"partition {i} node {node_id} token {tok}".encode('ascii'))
                hnode = xxh.digest()
                if h is None or hnode < h:
                    h = hnode
                    hn = node_id
                    hndc = node_dc
        return (i, hndc, hn)

    partition_nodes = [partition_node(i) for i in partitions]
    count_tokens_per_node(partition_nodes)

    dcs = list(set(node_dc for _, node_dc, _ in nodes))

        
    def walk_ring(v, rep):
        vh = hashlib.sha256(v).digest()
        i = (vh[0]<<8 | vh[1]) % (2**partition_bits)
        return walk_ring_from_pos(partition_nodes, dcs, i, rep)

    return walk_ring



def evaluate_method(walk_ring):
    node_data_counts = {}
    for i in range(100000):
        nodes = walk_ring(f"{i}".encode('ascii'), 3)
        for n in nodes:
            if n not in node_data_counts:
                node_data_counts[n] = 0
            node_data_counts[n] += 1
    print("Number of data items per node:")
    for n, v in sorted(list(node_data_counts.items())):
        print(n, ": ", v)


if __name__ == "__main__":

    nodes = [('digitale', 'atuin', 10),
             ('drosera', 'atuin', 10),
             ('datura', 'atuin', 10),
             ('io', 'jupiter', 20),
             ('meta', 'pipo', 10),
             ('mega', 'pipo', 10),
             ('mina', 'pipo', 10),
             #('moni', 'pipo', 10),
             #('mimi', 'pipo', 10),
             #('mesi', 'pipo', 10),
             ]

    print("------")
    print("method 1")
    method1_walk_ring = method1(nodes)
    evaluate_method(method1_walk_ring)

    print("------")
    print("method 2")
    method2_walk_ring = method2(nodes)
    evaluate_method(method2_walk_ring)
