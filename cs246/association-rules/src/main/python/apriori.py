from __future__ import division

import getopt
import sys
import itertools

"""
A solution for Stanford Winter 2014 CS246's Product Recommendation problem. It
generates correct recommendations for the browsing.txt dataset
"""


def parse_inputs(line):
    return frozenset() if line == '' else frozenset(line.split(' '))


def usage(exit_code=0):
    print "\nusage:\n"
    print "frequent-itemsets.py"
    print "  -i input_filepath"
    print "  -s support"
    sys.exit(exit_code)


def prune_non_frequent(items_counts, support):
    """
    Create an frequent itemset
    :param items_counts: a dictionary of items/counts
    :param support: support threshold
    :return: a dictionary representing the frequent itemset
    """
    return {k: v for k, v in items_counts.iteritems() if v >= support}


def candidate_k_tuples(frequent_items_set, k):
    """
    Create candidate items sets of size k using frequent items-sets of size k-1
    :param frequent_items_set: k-1 frequent items set
    :param k: the size of candidate items sets.
    :return:
    """
    candidates = {}
    for key_tuple in itertools.combinations(sorted(frequent_items_set), k):
        candidates[key_tuple] = 0

    return candidates


def count_candidate_k_tuples(baskets, candidate_tuples, k, items_set):
    """
    Generate frequencies of candidate items sets of size k
    :param baskets: list of baskets; each basket is a set of items
    :param candidate_tuples:
    :param k: cardinality of candidate sets
    :param items_set: frequent items for generating k-tuples of items
    :return:
    """
    for basket in baskets:
        basket_subset = sorted([item for item in basket if item in items_set])
        k_items = itertools.combinations(basket_subset, k)
        for item in k_items:
            if item in candidate_tuples:
                candidate_tuples[item] += 1


def find_frequent_pairs(baskets, frequent_singles, support):
    """
    Generate frequent associations of pairs of items { A->B }\

    :param baskets: list of baskets; each basket is a set of items
    :param frequent_singles: a dictionary representing frequent single items and their counts
    :param support:
    :return: a dictionary representing
    """
    candidate_pairs = candidate_k_tuples(frequent_singles, 2)
    count_candidate_k_tuples(baskets, candidate_pairs, 2, frequent_singles)
    return prune_non_frequent(candidate_pairs, support)


def find_frequent_triples(baskets, frequent_pairs, support):
    """
    Generate frequent associations of 3 items { A,B -> C }

    :param baskets: list of baskets; each basket is a set of items
    :param frequent_pairs: a dictionary representing frequent pairs of items and their counts
    :param support: support threshold
    :return:
    """
    items_set = set([f for fp in frequent_pairs for f in fp])
    candidate_triples = candidate_k_tuples(items_set, 3)
    count_candidate_k_tuples(baskets, candidate_triples, 3, items_set)
    return prune_non_frequent(candidate_triples, support)


def confidence(i, j):
    return j/i


def pair_confidence(freq_i, freq_j):
    """
    conf(I->j) for pairs, i.e., Support(Union(I,j))/Support(I), where I and J have cardinality of 1
    :param freq_i: a dictionary representing frequent single items and their counts
    :param freq_j: a dictionary representing frequent pairs of items and their counts
    :return:
    """
    t = []
    for k, v in freq_j.items():
        t.append(((k[0], k[1]), confidence(freq_i[k[0]], v)))  # conf(A->B)
        t.append(((k[1], k[0]), confidence(freq_i[k[1]], v)))  # conf(B->A)

    return t


def triple_confidence(i, j):
    """
    conf(I->j) for itemsets of cardinality > 2, i.e., Support(Union(I,j))/Support(I), where I has cardinality of k and J
    j has cardinality of 1

    Note: tested only for triples, thus the function name triple_confidence; will have to test
    for frequent items > 3

    :param i: dictionary representing frequent items of size k-1 and their counts
    :param j: dictionary representing frequent items of size k and their counts
    :return:
    """
    t = []
    n = len(j.iterkeys().next())
    for k, v in j.items():
        # for triples, generates Conf(A,B -> C), Conf(A,C->B) and Conf(B,C -> A) for triples if pairs are frequent
        ikeys = itertools.combinations(k, n-1)
        for ikey in ikeys:
            if ikey in i:
                t.append((tuple(list(ikey) + list(set(k) - set(ikey))), confidence(i[ikey], v)))

    return t


def main(args):
    input_filepath = None
    support = 0
    try:
        opts, args = getopt.getopt(args, "hi:s:")
        for o, a in opts:
            if o == "-h":
                usage()
            elif o == "-i":
                input_filepath = a
            elif o == "-s":
                support = int(a)
            else:
                usage(2)
    except getopt.GetoptError:
        usage(2)
    except ValueError:
        usage(2)

    if input_filepath is None or support == 0:
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
    print "Generate frequent pairs"
    frequent_pairs = find_frequent_pairs(baskets, frequent_singles, support)
    print "Generate frequent triples"
    frequent_triples = find_frequent_triples(baskets, frequent_pairs, support)

    print "Generate confidence of frequent itemsets of pairs"
    pc = pair_confidence(frequent_singles, frequent_pairs)
    pc.sort(key=lambda x: (-x[1], x[0]))

    print "Top 15 pairs by confidence"
    for pair in pc[:15]:
        print "{} -> {} {}".format(pair[0][0], pair[0][1], pair[1])

    print "Generate confidence of frequent itemsets of triples"
    tc = triple_confidence(frequent_pairs, frequent_triples)
    tc.sort(key=lambda x: (-x[1], x[0]))
    print "Top 15 triples by confidence"
    for triple in tc[:15]:
        print "{}, {} -> {} {}".format(triple[0][0], triple[0][1], triple[0][2], triple[1])

    print "done"

main(sys.argv[1:])
