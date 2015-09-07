from pyspark import SparkContext


def pair_tuples(line):
    """
    Converts input line to a list of pairs of friends
    :param line: person\tfriend1,friend2,....,friendn
    :return: [(person, friend1), (person, friend2),..., (person, friendn)]
    """
    tokens = line.split('\t')
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


def main():
    sc = SparkContext(appName="recommend-friends-1-py")
    file_rdd = sc.textFile('hdfs://localhost:8020/user/art/cs246/input/friendslist', 12)
    pairs_of_friends = file_rdd.flatMap(lambda line: pair_tuples(line))

    recommended_friends = (
        pairs_of_friends.join(pairs_of_friends).
            map(lambda (person, pair_of_friends): pair_of_friends).
            filter(lambda (person1, person2): person1 != person2).
            subtract(pairs_of_friends).
            map(lambda pair_with_a_mutual_friend: (pair_with_a_mutual_friend, 1)).
            reduceByKey(lambda a, b: a + b).
            map(lambda ((person1, person2), mutual_friends): (person1, (person2, mutual_friends))).
            groupByKey().
            mapValues(list).
            map(lambda person_and_strangers: recommend_new_friends(person_and_strangers))
    )

    recommended_friends. \
        map(lambda (person, recommendations): "{}\t{}".format(person,
                                                              ",".join(map(lambda x: str(x), recommendations)))). \
        saveAsTextFile("hdfs://localhost:8020/user/art/cs246/output/recommended-friends-1")


main()
