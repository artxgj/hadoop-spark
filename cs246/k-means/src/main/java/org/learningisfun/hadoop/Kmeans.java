package org.learningisfun.hadoop;

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

        private ArrayList<ArrayList<Double>> centroids;

        @Override
        public void setup(Context context) {
            try
            {
                centroids = new ArrayList<ArrayList<Double>>();
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
                System.err.println("Error reading state file.");
                ioe.printStackTrace();
                System.exit(1);
            }

        }


        private Double euclideanDistance(ArrayList<Double> coordinate, ArrayList<Double> centroid) {
            double sum = 0.0f;

            for (int i=0; i < coordinate.size(); i++)
                sum += Math.pow(coordinate.get(i) - centroid.get(i), 2);

            return Math.sqrt(sum);
        }


        private int nearestCentroid(ArrayList<Double> coordinate) {
            Double minVal = Double.MAX_VALUE;
            int centroidIndex = 0;

            int i = 0;
            for (ArrayList<Double> centroid: centroids) {
                Double d = euclideanDistance(coordinate, centroid);
                if (d < minVal) {
                    minVal = d;
                    centroidIndex = i;
                }
                ++i;
            }

            return centroidIndex;
        }

        private ArrayList<Double> lineToDoublesList(String line) {
            ArrayList<Double> fl = new ArrayList<Double>();
            String[] tokens = line.split(" ");

            for (String token:tokens) {
                fl.add(Double.valueOf(token.trim()));
            }

            return fl;
        }


        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            ArrayList<Double> featuresList = lineToDoublesList(value.toString());
            DoubleWritable[] dw = new DoubleWritable[featuresList.size()];
            int i =0;

            for (Double feature: featuresList) {
                dw[i++]  = new DoubleWritable(feature.doubleValue());
            }

            DoubleArrayWritable faw = new DoubleArrayWritable();
            faw.set(dw);
            int nc = nearestCentroid(featuresList);
            context.write(new IntWritable(nc), faw);
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
                    DoubleWritable x = (DoubleWritable)  dw[i];
                    sum[i] += x.get();
                }
            }
            // calculate new centroid
            Double[] centroid = new Double[sum.length];
            for (int i=0; i < sum.length; i++) {
                centroid[i] =  sum[i]/n;
            }
            String new_centroid  = StringUtils.join(centroid, ' ');
            context.write(new Text(new_centroid), new Text(""));
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
