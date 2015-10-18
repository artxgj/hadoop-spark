package org.learningisfun.hadoop;

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

public class Kmeans
{

    public static class FloatArrayWritable extends ArrayWritable {
        public FloatArrayWritable() {
            super(FloatWritable.class);
        }

    }


    public static class KmeansCentroidMapper extends
            Mapper<LongWritable, Text, IntWritable, FloatArrayWritable> {

        private ArrayList<ArrayList<Float>> centroids;

        @Override
        public void setup(Context context) {
            try
            {
                centroids = new ArrayList<ArrayList<Float>>();
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


        private float euclideanDistance(ArrayList<Float> coordinate, ArrayList<Float> centroid) {
            double sum = 0.0f;

            for (int i=0; i < coordinate.size(); i++)
                sum += Math.pow(coordinate.get(i) - centroid.get(i), 2);

            return (float) Math.sqrt(sum);
        }


        private int nearestCentroid(ArrayList<Float> coordinate) {
            float minVal = Float.MAX_VALUE;
            int centroidIndex = 0;

            int i = 0;
            for (ArrayList<Float> centroid: centroids) {
                float d = euclideanDistance(coordinate, centroid);
                if (d < minVal) {
                    minVal = d;
                    centroidIndex = i;
                }
            }

            return centroidIndex;
        }

        private ArrayList<Float> lineToFloatsList(String line) {
            ArrayList<Float> fl = new ArrayList<Float>();
            String[] tokens = line.split(" ");

            for (String token:tokens) {
                fl.add(Float.valueOf(token.trim()));
            }

            return fl;
        }


        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            ArrayList<Float> featuresList = lineToFloatsList(value.toString());
            FloatWritable[] fw = new FloatWritable[featuresList.size()];
            int i =0;

            for (Float feature: featuresList) {
                fw[i++]  = new FloatWritable(feature.floatValue());
            }

            FloatArrayWritable faw = new FloatArrayWritable();
            faw.set(fw);
            int nc = nearestCentroid(featuresList);
            context.write(new IntWritable(nc), faw);
        }

        void loadCentroids(String cacheFile) throws IOException {
            BufferedReader in = null;
            try {
                in = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile)));
                String line;
                while ((line = in.readLine()) != null) {
                    centroids.add(lineToFloatsList(line));
                }
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }


    public static class KmeansCentroidReducer extends
            Reducer<IntWritable, FloatArrayWritable, Text, Text> {
        @Override
        public void reduce(IntWritable Key, Iterable <FloatArrayWritable> values, Context context)
            throws IOException, InterruptedException {

            System.out.println("Reduce " + Key.get());
            for (FloatArrayWritable value: values) {
                Writable[] fw =  value.get();
                for (int i=0; i < fw.length; i++)
                    System.out.print(fw[i] + " ");
                System.out.println();

            }

            System.out.println("....");

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

        for (int i=1; i <= iterations; i++) {
            String outputCentroidsPath = String.format("%s/iter-%05d", outputBasePath, i);

            Job job = Job.getInstance();
            job.setJarByClass(KmeansCentroidMapper.class);
            job.setJobName(String.format("Kmeans centroids %05d", i));
            Path hdfsCentroidsPath = new Path(inputCentroidsPath);
            job.addCacheFile(hdfsCentroidsPath.toUri());
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(outputCentroidsPath));

            job.setMapperClass(KmeansCentroidMapper.class);
            job.setReducerClass(KmeansCentroidReducer.class);

            job.setMapOutputKeyClass( IntWritable.class );
            job.setMapOutputValueClass( FloatArrayWritable.class );

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
            inputCentroidsPath = outputCentroidsPath;
        }
    }
}
