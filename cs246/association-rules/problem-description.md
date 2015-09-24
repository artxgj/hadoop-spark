
## Stanford CS246: Mining Massive Data Sets Winter 2014 ##

[Problem Set 1, 2d and 2e](
http://snap.stanford.edu/class/cs246-2014/handouts.html)

Product Recommendations: The action or practice of selling additional products or services to existing customers is called cross-selling. Giving product recommendation is one of the examples of cross-selling that are frequently used by online retailers. One simple method to give product recommendations is to recommend products that are frequently browsed together by the customers.
Suppose we want to recommend new products to the customer based on the products they have already browsed on the online website. Write a program using the A-priori algorithm to find products which are frequently browsed together. Fix the support to s =100 (i.e. product pairs need to occur together at least 100 times to be considered frequent) and find itemsets of size 2 and 3.

Use the online browsing behavior dataset at: http://snap.stanford.edu/class/cs246-data/ browsing.txt. Each line represents a browsing session of a customer. On each line, each string of 8 characters represents the id of an item browsed during that session. The items are separated by spaces.

2d. List the top 5 rules with corresponding confidence scores in decreasing order of confidence score for itemsets of size 2. (Break ties, if any, by lexicographically increasing order on the left hand side of the rule.)

2e. List the top 5 rules with corresponding confidence scores in decreasing order of confidence score for itemsets of size 3. A rule is of the form: (item1, item2) ⇒ item3. (Order the left hand side pair lexicographically. Then break ties, if any, by lexicographical order of the first then the second item in the pair.)
