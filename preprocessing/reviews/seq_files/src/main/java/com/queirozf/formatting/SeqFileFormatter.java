package com.queirozf.formatting

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;


/**
 * A simple class to turn Json k-v pairs into sequencefiles, for consumption by Mahout
 */
class SeqFileFormatter {


    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("Usage: hadoop jar path/to/jar.jar SeqFileFormatter <inputpath> <outputpath>");
            System.exit(1);
        }

        // so we know what iteration we're in
        int iteration = 1;

        Configuration conf = new Configuration();

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);


        Job job = Job.getInstance(conf);

        job.setInputFormatClass(JsonInputFormat.class);
        job.setMapperClass(SeqFileMapper.class);
        job.setJarByClass(SeqFileFormatter.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);

        FileOutputFormat.setOutputPath(job, out);

        // these are the types of files we are reading
        job.setInputFormatClass(JsonInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(VectorWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.waitForCompletion(true);

    }


    class SeqFileMapper extends Mapper<LongWritable, MapWritable, LongWritable, VectorWritable> {
        public void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {

            // sorry about this
            double[] newSample = new double[99];

            int i = 0;

            for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
                double feature = Double.parseDouble(entry.getValue().toString());
                newSample[i] = feature;
                i++;
            }

            DenseVector dw = new DenseVector(newSample);

            VectorWritable vw = new VectorWritable(dw);

            context.write(key, vw);

        }
    }

}


