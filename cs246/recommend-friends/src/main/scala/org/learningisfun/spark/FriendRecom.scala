package org.learningisfun.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object FriendRecom {
  def tuples_of_friends(line: String): Array[(Int, Int)] = {
    val tokens = line.split('\t')

    if (tokens(0) == "" || tokens.length == 1)
      return Array.empty[(Int, Int)]

    val person = tokens(0).toInt

    tokens(1).
      split(",").
      map(friend => (person, friend.toInt))
  }

  def recommend_new_friends(people: List[(Int, Int)], n: Int) : List[Int]   = {
    people.sortBy(tup_pair => (-tup_pair._2, tup_pair._1))
      .take(n)
      .map(tup_pair => tup_pair._1)
  }

  def main(args: Array[String]) {
    if ( args.length != 3 ) {
        println("usage: FriendRecom input_uri output_uri partitions")
        sys.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("recommend-friends-scala"))
    val file_rdd=sc.textFile(args(0), args(2).toInt)
    val pairs_of_friends = file_rdd.flatMap(line => tuples_of_friends(line))

    val recommended_friends = pairs_of_friends.
      join(pairs_of_friends).
      map(elem => elem._2).
      filter(elem => elem._1 != elem._2).
      subtract(pairs_of_friends).
      map(pair_with_a_mutual_friend => (pair_with_a_mutual_friend, 1)).
      reduceByKey((a, b) => a + b).
      map(elem => (elem._1._1, (elem._1._2, elem._2))).
      groupByKey().
      map(tup2 => (tup2._1, recommend_new_friends(tup2._2.toList, 10))).
      map(tup2 => tup2._1.toString + "\t" + tup2._2.map(x=>x.toString).toArray.mkString(",")).
      saveAsTextFile(args(1))
  }
}
