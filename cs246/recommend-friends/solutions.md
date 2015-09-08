## Solutions

### Description

**Input file:** &lt;User&gt;&lt;TAB&gt;&lt;Friends&gt;

The list of friends &lt;Friends&gt; in each line of the input file can be thought of as strangers whose mutual friend is &lt;User&gt;. Pairs of strangers can be generated from each line of input file. The number of times each pair of strangers appear in the entire file tells us how many mutual friends each pair has. One thing to note is "strangers" that appear in one line's <FRIENDS> could appear as friends in another line of the file. For example:

    1<TAB>2,3,...
    2<TAB>1,3,...
    3<TAB>1,2,...

    In line 1, (2, 3)'s mutual friend is 1. Lines 2 and 3 show that 2 and 3 are friends.

All pairs of friends have to be excluded from list of pairs of strangers.

After identifying the pairs of strangers with a mutual friend, count how many mutual friends each pair of strangers have, which can be expressed as a tuple containing a tuple of user ids and a count, e.g. ((1, 2), 10).

Transform the list of these tuples into tuples that look like (id1, (id2, count)), e.g., ((1,2), 10 ) becomes (1, (2, 10)). Now the key is an id instead of a tuple of two ids. As such, grouping the id keys together results in a list of potential friends for each person.

I implemented two solutions for this problem. The two solutions differ in how they generate pairs of strangers with a mutual friend. The second solution solution 2 borrowed Stanford's CS246 Hadoop solution for identifying pairs of friends and pairs of people who share a mutual friend.

Solution 1 was first implemented in Python and then in Scala. Solution 2 was implemented in Python.

#### Solution 1

1. Generate a RDD of pairs of friends.
2. Self-join the RDD.
3. Use the values (pairs of people) of the self-joined RDD to generate a new RDD.
4. Remove pairs of friends [1] from the pairs of people [3].
5. Filter out pairs that have the same id


#### Solution 2
Pairs of people are identified as friends with a 0 flag, while pairs of strangers who share a mutual friend are identified with a 1 flag.

    (1,2),1
    (4,5),0
    (6,7),1
    (6,7),

    (1,2) share a mutual friend and (4,5) are friends. (6,7) were initially identified as having a mutual friend and later found to be friends as well.

1. Generate a RDD of pairs of friends and pairs of people with a mutual friend.
2. Generate a RDD of pairs of friends.
3. Generate a RDD of pairs of "stangers" by removing friends RDD [2] from the RDD of pairs of people [1]. The remainder will have entries that look like "(p1, p2), 1"
