from pyspark import SparkContext


def map_relations(line):
    x = line.split('\t')
    person = int(x[0])
    friends = x[1].split(',')

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


def main():
    sc = SparkContext(appName="recommend-friends-2-py")
    file_rdd = sc.textFile('hdfs://localhost:8020/user/art/cs246/input/friendslist', 12)
    relationship_pairs = file_rdd.flatMap(lambda line: map_relations(line))
    already_friends = relationship_pairs.filter(lambda (pair, relationship): relationship == 0)

    recommended_friends = (
        relationship_pairs.
            subtractByKey(already_friends).
            reduceByKey(lambda a, b: a + b).
            map(lambda ((person1, person2), mutual_friends): (person1, (person2, mutual_friends))).
            groupByKey().
            mapValues(list).
            map(lambda person_and_strangers: recommend_new_friends(person_and_strangers))
    )

    recommended_friends. \
        map(lambda (person, recommendations): "{}\t{}".format(person,
                                                              ",".join(map(lambda x: str(x), recommendations)))). \
        saveAsTextFile("hdfs://localhost:8020/user/art/cs246/output/recommended-friends-2")


main()
