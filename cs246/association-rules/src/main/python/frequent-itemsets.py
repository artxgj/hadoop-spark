from __future__ import division

import getopt
import sys
import itertools


def parse_inputs(line):
    return frozenset() if line == '' else frozenset(line.split(' '))


def usage(exit_code=0):
    print "\nusage:\n"
    print "frequent-itemsets.py"
    print "  -i input_filepath"
    print "  -o output_filepath"
    print "  -s support"
    sys.exit(exit_code)


def confidence(i, j):
    return j/i


def pair_confidence(freq_i, freq_j):
    t = []
    for k, v in freq_j.items():
        t.append(((k[0], k[1]), confidence(freq_i[k[0]], v)))
        t.append(((k[1], k[0]), confidence(freq_i[k[1]], v)))

    return t


def triple_confidence(i, j):
    t = []
    n = len(j.iterkeys().next())
    for k, v in j.items():
        ikeys = itertools.combinations(k, n-1)
        for ikey in ikeys:
            if ikey in i:
                t.append((tuple(list(ikey) + list(set(k) - set(ikey))), confidence(i[ikey], v)))

    return t


def prune_non_frequent(items_counts, support):
    return {k: v for k, v in items_counts.iteritems() if v >= support}


def candidate_k_tuples(items_set, k):
    candidates = {}
    for key_tuple in itertools.combinations(sorted(items_set), k):
        candidates[key_tuple] = 0

    return candidates


def frequent_k_tuples(baskets, candidate_tuples, k, items_set):
    for basket in baskets:
        basket_subset = sorted([item for item in basket if item in items_set])
        k_items = itertools.combinations(basket_subset, k)
        for item in k_items:
            if item in candidate_tuples:
                candidate_tuples[item] += 1


def find_frequent_pairs(baskets, frequent_singles, support):
    candidate_pairs = candidate_k_tuples(frequent_singles, 2)
    frequent_k_tuples(baskets, candidate_pairs, 2, frequent_singles)
    return prune_non_frequent(candidate_pairs, support)


def find_frequent_triples(baskets, frequent_pairs, support):
    triple_items_set = set([f for fp in frequent_pairs for f in fp])
    candidate_triples = candidate_k_tuples(triple_items_set, 3)
    frequent_k_tuples(baskets, candidate_triples, 3, triple_items_set)
    return prune_non_frequent(candidate_triples, support)


def main(args):
    output_filepath = input_filepath = None
    support = 0
    try:
        opts, args = getopt.getopt(args, "hi:o:s:")
        for o, a in opts:
            if o == "-h":
                usage()
            elif o == "-i":
                input_filepath = a
            elif o == "-o":
                output_filepath = a
            elif o == "-s":
                support = int(a)
            else:
                usage(2)
    except getopt.GetoptError:
        usage(2)
    except ValueError:
        usage(2)

    if input_filepath is None or output_filepath is None or support == 0:
        usage(2)

    print "process singles"
    singles = {}
    baskets = []
    with open(input_filepath, 'r') as f:
        for line in f:
            basket = line.strip().split(' ')
            if basket[0] == '':
                continue
            for item in basket:
                if item in singles:
                    singles[item] += 1
                else:
                    singles[item] = 1
            baskets.append(set(basket))

    frequent_singles = prune_non_frequent(singles, support)
    print "process pairs"
    frequent_pairs = find_frequent_pairs(baskets, frequent_singles, support)
    print "process triples"
    frequent_triples = find_frequent_triples(baskets, frequent_pairs, support)

    print "Top pairs by confidence"
    pc = pair_confidence(frequent_singles, frequent_pairs)
    pc.sort(key=lambda x: (-x[1], x[0]))

    for pair in pc[:15]:
        print "{} -> {} {}".format(pair[0][0], pair[0][1], pair[1])

    print
    print "Top triples by confidence"
    tc = triple_confidence(frequent_pairs, frequent_triples)
    tc.sort(key=lambda x: (-x[1], x[0]))
    for triple in tc[:15]:
        print "{}, {} -> {} {}".format(triple[0][0], triple[0][1], triple[0][2], triple[1])

    print "done"

main(sys.argv[1:])
