from pyspark import SparkContext
import getopt
import sys


def gen_pairs_of_friends(line):
    """
    Converts input line to a list of pairs of friends
    :param line: person\tfriend1,friend2,....,friendn
    :return: [(person, friend1), (person, friend2),..., (person, friendn)]
    """
    tokens = line.split('\t')
    if len(tokens) == 1 or tokens[0] == '':
        return []    # no friends or incorrectly formatted line

    person = int(tokens[0])
    friends = tokens[1].split(',')

    # friend = '' means person does not have a friend
    return [(person, int(friend)) for friend in friends if friend != '']


def recommend_new_friends(person_and_strangers, n=10):
    """

    :param person_and_strangers: (p, [(s1, num_mutual_friends), (s2, num_mutual_friends),...(sN, num_mutual_friends))
    :param n: number of friends to recommend
    :return: a list of up to n strangers to recommend as friends
    """
    person, list_of_strangers = person_and_strangers

    # list_of_strangers: [(stranger1, number of mutual friends), ..., (strangerN, number of mutual friends)]
    ordered_list_of_strangers = sorted(list_of_strangers,
                                       key=lambda (stranger, mutual_friends): (-mutual_friends, stranger))[:n]
    recommendations = map(lambda (stranger, mutual_friends): stranger, ordered_list_of_strangers)
    return person, recommendations


def usage(exit_code=0):
    print "\nusage:\n"
    print "recommend-friends-1.py"
    print "  -i input_filepath"
    print "  -o output_filepath"
    print "  -p <partitions>\tdefault value is 12\n"
    sys.exit(exit_code)


def main(args):
    output_filepath = input_filepath = None
    partitions = 12
    try:
        opts, args = getopt.getopt(args, "hi:o:p:")
        for o, a in opts:
            if o == "-h":
                usage()
            elif o == "-i":
                input_filepath = a
            elif o == "-o":
                output_filepath = a
            elif o == "-p":
                partitions = int(a)
            else:
                usage(2)
    except getopt.GetoptError:
        usage(2)
    except ValueError:
        usage(2)

    print input_filepath, output_filepath
    if input_filepath is None or output_filepath is None:
        usage(2)

    sc = SparkContext(appName="recommend-friends-1-py")
    file_rdd = sc.textFile(input_filepath, partitions)
    pairs_of_friends = file_rdd.flatMap(lambda line: gen_pairs_of_friends(line))

    (pairs_of_friends.join(pairs_of_friends).
     map(lambda (person, pair_of_friends): pair_of_friends).
     filter(lambda (person1, person2): person1 != person2).
     subtract(pairs_of_friends).
     map(lambda pair_with_a_mutual_friend: (pair_with_a_mutual_friend, 1)).
     reduceByKey(lambda a, b: a + b).
     map(lambda ((person1, person2), mutual_friends): (person1, (person2, mutual_friends))).
     groupByKey().
     mapValues(list).
     map(lambda person_and_strangers: recommend_new_friends(person_and_strangers)).
     map(lambda (person, recommendations): "{}\t{}".format(person, ",".join(map(lambda x: str(x), recommendations)))).
     saveAsTextFile(output_filepath)
     )


main(sys.argv[1:])
