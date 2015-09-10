import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.util.Vector;
import java.util.Arrays;
import java.util.List;

public class NaiveKMeansReducer extends Reducer<VectorWritable,VectorWritable,VectorWritable,VectorWritable> {
    
    private final Vector<VectorWritable> centers = new Vector<VectorWritable>();

    public static enum Counter{
        CONVERGED
    }

    public void reduce(VectorWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
      
      Vector<VectorWritable> vectorList = new Vector<VectorWritable>();

      Vector<Double> newCenter = null;

      double epsilon = context.getConfiguration().getDouble("epsilon",0.001d);

      // these are all the points that are in cluster key
      for(VectorWritable sample: values){

        vectorList.add(new VectorWritable(sample));

        // in the first loop, just set a point as a center
        if(newCenter == null){
          newCenter = Utils.deepCopy(sample.getVector());
        }else{
          //now add the current center and the sample so as to get the new center
          newCenter = Utils.add(newCenter,sample.getVector());
        }

      }

      // System.out.println("new center before division: "+newCenter);

      newCenter = Utils.divide(newCenter,vectorList.size());

      // System.out.println("new center after division: "+newCenter);
      //System.out.println("vectorList.size() is"+vectorList.size());

      VectorWritable center = new VectorWritable(newCenter);

      centers.add(center);

      for (VectorWritable vector : vectorList) {
        context.write(center, vector);
      }

      if (Utils.converged(center.getVector(),key.getVector(),epsilon)){
        context.getCounter(Counter.CONVERGED).increment(1);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException,InterruptedException {
      super.cleanup(context);

      Configuration conf = context.getConfiguration();
      Path centersPath = new Path(conf.get("centers.path"));
      FileSystem fs = FileSystem.get(conf);
      
      // delete old centers
      fs.delete(centersPath, true);

      try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), centersPath, VectorWritable.class, IntWritable.class)) {
      
        final IntWritable value = new IntWritable(0);

        for (VectorWritable ctr : centers) {
          // write new, updated centers
          out.append(ctr, value);
        }
      }
    }

}