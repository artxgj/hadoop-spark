package org.learningisfun.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object FriendRecomV2019 {
  def pairs_of_friends(user_friends_line: String): Array[((Int, Int), Int)] = {
    val tokens = user_friends_line.split('\t')

    if (tokens(0) == "" || tokens.length == 1)
      return Array.empty[((Int, Int), Int)]

    val user = tokens(0).toInt
    val friends = tokens(1)

    friends.split(",").
      map(friend => ((user, friend.toInt), 0))
  }

  def candidate_pairs_with_mutual_friends(user_friends_line: String): Array[((Int, Int), Int)]  = {
    val tokens = user_friends_line.split('\t')

    if (tokens(0) == "" || tokens.length == 1)
      return Array.empty[((Int, Int), Int)]

    val friends = tokens(1).split(",").map(friend => friend.toInt)
    var new_pairs = ListBuffer.empty[((Int, Int), Int)]
    for {
      i <- 0 until friends.length
      j <- 0 until friends.length
      if i != j
    } new_pairs.append(((friends(i), friends(j)), 1))

    return new_pairs.toArray
  }

  def recommend_new_friends(people: List[(Int, Int)], max_recommendations: Int) : List[Int]   = {
     people.sortBy(person_mutualfriendscount => (-person_mutualfriendscount._2, person_mutualfriendscount._1))
      .take(max_recommendations)
      .map(person_mutualfriendscount=> person_mutualfriendscount._1)
  }

  def main(args: Array[String]) {
    if ( args.length != 4 ) {
        println("usage: FriendRecomV2019 V2019 input_uri output_uri max_recommendations partitions")
        sys.exit(-1)
    }

    val input_uri = args(0)
    val output_uri = args(1)
    val max_recommendations = args(2).toInt
    val spark_partitions = args(3).toInt

    val sc = new SparkContext(new SparkConf().setAppName("recommend-friends-2019-scala"))
    val file_rdd=sc.textFile(input_uri, spark_partitions)
    val already_friends = file_rdd.flatMap(line => pairs_of_friends(line))
    val possible_pairs_with_mutualfriend = file_rdd.flatMap(line => candidate_pairs_with_mutual_friends(line))

    // The following is an equivalent of "A left outer join B on A.id = B.id where B.id is NULL"
    val pairs_with_mutualfriend = possible_pairs_with_mutualfriend.leftOuterJoin(already_friends)
                                    .filter(pair => pair._2._2 == None)
                                    .map(pair => ((pair._1._1, pair._1._2), pair._2._1) )

    val recommended_friends = pairs_with_mutualfriend.
      reduceByKey((a, b) => a + b).
      map(two_with_mutualfriends => (two_with_mutualfriends._1._1, (two_with_mutualfriends._1._2, two_with_mutualfriends._2))).
      groupByKey().
      map(tup2 => (tup2._1, recommend_new_friends(tup2._2.toList, max_recommendations))).
      map(tup2 => tup2._1.toString + "\t" + tup2._2.map(x=>x.toString).toArray.mkString(",")).
      saveAsTextFile(output_uri)
  }
}
