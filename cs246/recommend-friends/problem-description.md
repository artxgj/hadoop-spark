## Stanford CS246: Mining Massive Data Sets Winter 2014 ##

[Problem Set 1](
http://snap.stanford.edu/class/cs246-2014/handouts.html)

1. MapReduce (25 pts) [James/Janice/Jean-Yves]

Write a MapReduce program in Hadoop that implements a simple “People You Might Know” social network friendship recommendation algorithm. The key idea is that if two people have a lot of mutual friends, then the system should recommend that they connect with each other.

**Input:**
Download the input file from the link: http://snap.stanford.edu/class/cs246-data/hw1q1.zip.
The input file contains the adjacency list and has multiple lines in the following format:

&lt;User&gt;&lt;TAB&gt;&lt;Friends&gt;

Here, &lt;User&gt; is a unique integer ID corresponding to a unique user and &lt;Friends&gt; is a comma separated list of unique IDs corresponding to the friends of the user with the unique
￼ID &lt;User&gt;. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge.

**Algorithm:** Let us use a simple algorithm such that, for each user U, the algorithm recommends N = 10 users who are not already friends with U, but have the most number of mutual friends in common with U.

**Output:** The output should contain one line per user in the following format:

&lt;User&gt;&lt;TAB&gt;&lt;Recommenations&gt;

where &lt;User&gt; is a unique ID corresponding to a user and &lt;Recommendations&gt; is a comma separated list of unique IDs corresponding to the algorithm’s recommendation of people that <User> might know, ordered in decreasing number of mutual friends. Even if a user has less than 10 second-degree friends, output all of them in decreasing order of the number of mutual friends. If a user has no friends, you can provide an empty list of recommendations. If there are recommended users with the same number of mutual friends, then output those user IDs in numerically ascending order.
Also, please provide a description of how you are going to use MapReduce jobs to solve this problem. Don’t write more than 3 to 4 sentences for this: we only want a very high-level description of your strategy to tackle this problem.

**Note:** It is possible to solve this question with a single MapReduce job. But if your solution requires multiple map reduce jobs, then that’s fine too.
