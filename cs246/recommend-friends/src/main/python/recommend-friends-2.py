from pyspark import SparkContext
import getopt
import sys


def map_relations(line):
    tokens = line.split('\t')
    if len(tokens) == 1 or tokens[0] == '':
        return []    # no friends or incorrectly formatted line
    person = int(tokens[0])
    friends = tokens[1].split(',')

    # friend = '' means person does not have a friend
    friends_list = [((person, int(friend)), 0) for friend in friends if friend != '']
    mutual_friend_list = []

    # person is a mutual friend of all of person's friends
    for i in range(0, len(friends) - 1):
        for j in range(i + 1, len(friends)):
            a = int(friends[i])
            b = int(friends[j])
            mutual_friend_list.append(((a, b), 1))
            mutual_friend_list.append(((b, a), 1))

    return friends_list + mutual_friend_list


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
    print "recommend-friends-2.py"
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

    if input_filepath is None or output_filepath is None:
        usage(2)

    sc = SparkContext(appName="recommend-friends-2-py")
    file_rdd = sc.textFile(input_filepath, partitions)
    relationship_pairs = file_rdd.flatMap(lambda line: map_relations(line))
    already_friends = relationship_pairs.filter(lambda (pair, relationship): relationship == 0)

    # recommend friends
    (relationship_pairs.
     subtractByKey(already_friends).
     reduceByKey(lambda a, b: a + b).
     map(lambda ((person1, person2), mutual_friends): (person1, (person2, mutual_friends))).
     groupByKey().
     mapValues(list).
     map(lambda person_and_strangers: recommend_new_friends(person_and_strangers)).
     map(lambda (person, recommendations): "{}\t{}".format(person, ",".join(map(lambda x: str(x), recommendations)))).
     saveAsTextFile(output_filepath)
     )


main(sys.argv[1:])
