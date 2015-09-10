import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.util.Vector;
import java.util.Arrays;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NaiveKMeans{

  public static void main(String[] args) throws Exception{

    if(args.length < 4){
      System.out.println("Usage: hadoop jar path/to/jar.jar NaiveKMeans <inputpath> <outputpath> <k> <dimensions>");
      System.exit(1);
    }

    // so we know what iteration we're in
    int iteration = 1;

    Configuration conf = new Configuration();
    conf.setInt("num.iteration", iteration);

    Path centerPath = new Path("myfiles/clustering/import/center/cen.seq");

    // this is where the updated centers are written every iteration
    conf.set("centers.path", centerPath.toString());
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    int k = Integer.parseInt(args[2]);
    int dimensions = Integer.parseInt(args[3]);

    double epsilon = 0.001d;

    conf.setInt("num.clusters",k);
    conf.setInt("num.dimensions",dimensions);
    conf.setDouble("epsilon",epsilon);

    Job job = Job.getInstance(conf);

    // job.setInputFormatClass(JsonInputFormat.class);
    job.setMapperClass(NaiveKMeansMapper.class);
    job.setReducerClass(NaiveKMeansReducer.class);
    job.setJarByClass(NaiveKMeansMapper.class);

    FileInputFormat.addInputPath(job,in);
    FileSystem fs = FileSystem.get(conf);

    // delete, maybe it's full b/c of previous runs
    // if (fs.exists(out)) {
    //   fs.delete(out, true);
    // }

    if (fs.exists(centerPath)) {
      fs.delete(out, true);
    }

    writeInitialCenters(conf,centerPath,fs);

    FileOutputFormat.setOutputPath(job,out);

    // these are the types of files we are reading
    job.setInputFormatClass(JsonInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(VectorWritable.class);
    job.setOutputValueClass(VectorWritable.class);

    job.waitForCompletion(true);

    long converged = job.getCounters().findCounter(NaiveKMeansReducer.Counter.CONVERGED).getValue();

    iteration++;

    // this is the actual iteration job
    // will run as long as it doesn't converge
    while(converged > 0){

      System.out.println("begin of iteration: "+iteration);

      conf = new Configuration();

      conf.set("centers.path", centerPath.toString());
      conf.setInt("num.iteration", iteration);
      conf.setDouble("epsilon",epsilon);

      job = Job.getInstance(conf);
      job.setJobName("Iteration:" + iteration);

      FileInputFormat.addInputPath(job,in);
      FileOutputFormat.setOutputPath(job,out);

      job.setMapperClass(NaiveKMeansMapper.class);
      job.setReducerClass(NaiveKMeansReducer.class);
      job.setJarByClass(NaiveKMeansMapper.class);

      job.setOutputKeyClass(VectorWritable.class);
      job.setOutputValueClass(VectorWritable.class);

      // centers found in the previous iteration
      // in = new Path("myfiles/clustering/iteration_" + (iteration - 1) + "/");
      out = new Path(args[1] +"__"+ iteration);

      // if (fs.exists(out))
      //   fs.delete(out, true);

      FileOutputFormat.setOutputPath(job,out);

      job.setInputFormatClass(JsonInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setOutputKeyClass(VectorWritable.class);
      job.setOutputValueClass(VectorWritable.class);

      job.waitForCompletion(true);
      iteration++;

      converged = job.getCounters().findCounter(NaiveKMeansReducer.Counter.CONVERGED).getValue();

    }

    // after all iterations, the result is stored in the output for the last iteration
    
  }

  public static void writeInitialCenters(Configuration conf, Path centers, FileSystem fs) throws IOException {

    try (SequenceFile.Writer w = SequenceFile.createWriter(fs, conf, centers, VectorWritable.class,IntWritable.class)) {
      
      final IntWritable value = new IntWritable(0);

      int k = conf.getInt("num.clusters",3);
      int dimensions = conf.getInt("num.dimensions",7);

      // the first clusters are simple
      for(int i=0; i<k ; i++){

        Vector<Double> randomVec = Utils.getRandomNormalizedVector(dimensions);

        // one center and a dummy number
        w.append(new VectorWritable(randomVec),new IntWritable(i));
      }
    }
  }
}
