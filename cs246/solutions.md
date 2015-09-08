## Solutions

### Description

**Input file:** &lt;User&gt;&lt;TAB&gt;&lt;Friends&gt;

The list of friends &lt;Friends&gt; in each line of the input file can be thought of as strangers whose mutual friend is &lt;User&gt;. Pairs of strangers can be generated from each line of input file. The number of times each pair of strangers appear in the entire file tells us how many mutual friends each pair has. One thing to note is "strangers" that appear in one line's <FRIENDS> could appear as friends in another line of the file. For example:

    1<TAB>2,3,...
    2<TAB>1,3,...
    3<TAB>1,2,...

    In line 1, (2, 3)'s mutual friend is 1. Lines 2 and 3 show that 2 and 3 are friends.

All pairs of friends have to be excluded from list of pairs of strangers.


#### Solution 1
1. Generate a RDD of pairs of friends.
2. Self-join the RDD.
3. Use the values (pairs of people) of the self-joined RDD to generate a new RDD.
4. Remove pairs of friends (1) from the pairs of people (3).
5. Filter out pairs that have the same id


#### Solution 2
Pairs of people are identified as friends with a 0 flag, while pairs of strangers who share a mutual friend are identified with a 1 flag.
    (1,2),1
    (4,5),0

    1 and 2 share a mutual friend. 4 and 5 are friends.

1. Generate a RDD of pairs of friends and pairs of people with a mutual friend.
2. Remove the pairs of friends.
