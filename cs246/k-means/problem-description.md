## Stanford CS246: Mining Massive Data Sets Winter 2014 ##

[Problem Set 2, no. 4](http://web.stanford.edu/class/cs246/homeworks/hw2.pdf)

Automatic tagging and cross-linking of documents: Clustering algorithms are frequently used to automatically categorize documents by web services which receive a large number of new content items daily. As human intervention would be very expensive, these services need an automated way to find related items and to categorize and cross-link the documents. News-aggregation site feedly is an example service which processes and categorizes tens of thousands of new documents per day.

We will explore this use case in this problem.

**(a)**

k-Means clustering on Hadoop: Implement the k-means using Map Reduce where a single step of Map Reduce completes one iteration of the k-means algorithm. So, to run k-means for i iterations, you will have to run a sequence of i MapReduce jobs.

Run the k-Means algorithm on Hadoop to find the centroids for the dataset at: http: //snap.stanford.edu/class/cs246-data/hw2-q4-kmeans.zip

The zip has 4 files:
  1. data.txt contains the dataset which has 4601 rows and 58 columns. Each row is a document represented as a 58 dimensional vector of features. Each component in the vector represents the importance of a word in the document.
  2. vocab.txt contains the words used for generating the document vector. (For example the first word represents the first feature of the document vector. For document 2 (line 2 in data.txt), feature 3 “alkalemia” (line 3 in vocab.txt) has frequency 0.5, so the third component of the document vector is 0.5.)
  3. c1.txt contains k initial cluster centroids. These centroids were chosen by selecting k = 10 random points from the input data.

  4. c2.txt contains initial cluster centroids which are as far apart as possible. (You can do this by choosing 1st centroid c1 randomly, and then finding the point c2 that is farthest from c1, then selecting c3 which is farthest from c1 and c2, and so on).

Use Euclidean distance (ie, the L2 norm) as the distance measure. Set number of iterations to 20 and number of clusters to 10. Use points in c1.txt for initialization.

**Hint about job chaining:**

We need to run a sequence of Hadoop jobs where the output of one job will be the input for the next one. There are multiple ways to do this and you are free to use any method you are comfortable with. One simple way to handle a such a multistage job is to configure the output path of the first job to be the input path of the second and so on.

The following pseudo code demonstrates job chaining.
```
var inputDir
var outputDir
var centroidDir
for i in no-of-iterations (
     Configure job here with all params
     Set job input directory = inputDir
     Set job output directory = outputDir + i
     Run job
     centroidDir =  outputDir + i
)
```
You will also need to share the location of centroid file with the mapper. There are many ways to do this and you can use any method you find suitable. One way is to use the Hadoop Configuration object. You can set it as a property in the Configuration object and retrieve the property value in the Mapper setup function.

For more details see :
￼
1. http://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/conf/Configuration. html#set(java.lang.String,java.lang.String)
2. http://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/conf/Configuration. html#get(java.lang.String)
3. http://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/mapreduce/Mapper. html#setup(org.apache.hadoop.mapreduce.Mapper.Context)


**Cluster Initialization:** The output of k-Means algorithm depends on the initial points chosen. There are many ways of choosing the initial points. We will compare two of them: random selection, and selecting points as far apart as possible.


**(b)**

For every iteration, compute the cost function φ(i) (as defined at the beginning of question 3). This means that, for your first MapReduce job iteration, you’ll be computing the cost function using the initial centroids located in one of the two text files. Run the k-means on data.txt using c1.txt and c2.txt. Generate a graph where you plot the cost function φ(i) as a function of the number of iterations i=1..20 for c1.txt and also for c2.txt.
(Hint: Note that you do not need to write a separate MapReduce job to do this. You can incorporate the computation of φ(i) into the Mapper/Reducer from part (a).)


**(c)**

Is random initialization of k-means using c1.txt better than initialization using c2.txt in terms of cost φ(i)? Why? What is the percentage change in cost from using the initial centroids versus using those centroids obtained after 10 iterations of K-Means?
Automatically tagging documents: One way of tagging documents is to assign word tags to each cluster. To get the top k word tags for a cluster, sort the features for its centroid by their coordinate values in descending order, choose the top k features and use words from vocab.txt to get the word tags for these top k features.
For e.g. if the indexes of top 5 features of the centroid for cluster 1 are [1, 2, 6, 7, 10] then the tags for the documents in cluster 1 are [activator, decay-corrected, softer, mitomycin, spc] which are words at line 1, 2, 6, 7, 10 respectively in vocab.txt.
Use points in c1.txt for initialization.

**(d)**

Which 5 tags would you apply to the second document? For verification, the 10th tag is alkalemia.
