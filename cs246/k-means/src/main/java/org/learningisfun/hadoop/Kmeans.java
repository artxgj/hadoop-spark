package org.learningisfun.hadoop;

/**
 *  First iteration of program: pass centroids file to Distributed cachedgenerate and generation new centroids
 *
 */
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Kmeans
{

    public static class DoubleArrayWritable extends ArrayWritable {
        public DoubleArrayWritable() {
            super(DoubleWritable.class);
        }
    }


    public static class KmeansCentroidMapper extends
            Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {

        private ArrayList<double[]> centroids;

        @Override
        public void setup(Context context) {
            try
            {
                centroids = new ArrayList<double[]>();
                URI[] cacheFiles = context.getCacheFiles();
                for (URI cacheFile:cacheFiles) {
                    String filepath = cacheFile.toString();
                    String filename = filepath.substring(filepath.lastIndexOf('/') + 1);
                    loadCentroids(filename);
                }
                System.out.println("Number of centroids = " + centroids.size());
            }
            catch (IOException ioe)
            {
                System.err.println("Error reading centroids file.");
                ioe.printStackTrace();
                System.exit(1);
            }

        }

        private double euclideanDistance(double[] coordinate, double[] centroid) {
            double sum = 0.0;

            for (int i=0; i < coordinate.length; i++)
                sum += Math.pow(coordinate[i] - centroid[i], 2);

            return Math.sqrt(sum);
        }


        private int nearestCentroid(double[] coordinate) {
            double minVal = Double.MAX_VALUE;
            int centroidIndex = 0;

            int i = 0;
            for (double[] centroid: centroids) {
                double d = euclideanDistance(coordinate, centroid);
                if (d < minVal) {
                    minVal = d;
                    centroidIndex = i;
                }
                ++i;
            }

            return centroidIndex;
        }

        private double[] lineToDoublesList(String line) {
            String[] tokens = line.trim().split(" ");
            double[] d = new double[tokens.length];
            int i=0;

            for (String token:tokens) {
                d[i++] =  Double.valueOf(token.trim());
            }

            return d;
        }


        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            double[] featuresList = lineToDoublesList(value.toString());
            DoubleWritable[] dw = new DoubleWritable[featuresList.length];
            for (int i=0; i < featuresList.length; i++) {
                dw[i]  = new DoubleWritable(featuresList[i]);
            }

            DoubleArrayWritable daw = new DoubleArrayWritable();
            daw.set(dw);
            int nc = nearestCentroid(featuresList);
            context.write(new IntWritable(nc), daw);
        }

        void loadCentroids(String cacheFile) throws IOException {
            BufferedReader in = null;
            try {
                in = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile)));
                String line;
                while ((line = in.readLine()) != null) {
                    centroids.add(lineToDoublesList(line));
                }
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }


    public static class KmeansCentroidReducer extends
            Reducer<IntWritable, DoubleArrayWritable, Text, Text> {
        private DecimalFormat df = new DecimalFormat("#.###");

        @Override
        public void reduce(IntWritable Key, Iterable <DoubleArrayWritable> values, Context context)
            throws IOException, InterruptedException {

            int n = 0;
            double[] sum = null;
            for (DoubleArrayWritable value: values) {
                ++n;
                Writable[] dw =  value.get();
                if (sum == null) {
                    sum = new double[dw.length];
                    Arrays.fill(sum, 0.0);
                }

                for (int i=0; i < dw.length; i++) {
                    DoubleWritable x = (DoubleWritable) dw[i];
                    sum[i] += x.get();
                }
            }
            // calculate new centroid

            Double[] centroid = new Double[sum.length];
            for (int i=0; i < sum.length; i++) {
                centroid[i] =  Double.valueOf(df.format(sum[i]/n));
            }
            String new_centroid  = StringUtils.join(centroid, ' ');
            context.write(new Text(""), new Text(new_centroid));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 4) {
            System.err.println("Missing arguments.");
            System.exit(-1);
        }

        int iterations = Integer.parseInt(args[0]);
        String dataPath = args[1];
        String inputCentroidsPath = args[2];
        String outputBasePath = args[3];
        String outputCentroidsPath = null;

        for (int i=1; i <= iterations; i++) {
            outputCentroidsPath = String.format("%s/iter-%05d", outputBasePath, i);

            Job job = Job.getInstance();
            job.setJarByClass(KmeansCentroidMapper.class);
            job.setJobName(String.format("Kmeans centroids %05d", i));
            Path hdfsCentroidsPath = new Path(inputCentroidsPath);
            job.addCacheFile(hdfsCentroidsPath.toUri());
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(outputCentroidsPath));

            job.setMapperClass(KmeansCentroidMapper.class);
            job.setReducerClass(KmeansCentroidReducer.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass( IntWritable.class );
            job.setMapOutputValueClass( DoubleArrayWritable.class );

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            if (outputCentroidsPath != null) {
                Configuration config = new Configuration();
                FileSystem fs =  FileSystem.get(config);
                FileStatus[] files = fs.listStatus(new Path(outputCentroidsPath));

                for (FileStatus file:files) {
                    if (file.isFile() && file.getPath().getName().startsWith("part")) {
                        inputCentroidsPath = new String(outputCentroidsPath + "/" + file.getPath().getName());
                        break;
                    }
                }
            }
        }
    }
}
