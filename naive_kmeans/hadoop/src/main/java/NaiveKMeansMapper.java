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

public class NaiveKMeansMapper extends Mapper<LongWritable, MapWritable, VectorWritable, VectorWritable>{

  private final Vector<VectorWritable> centers = new Vector<VectorWritable>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    Path centersPath  = new Path(conf.get("centers.path"));
    FileSystem fs = FileSystem.get(conf);

    // reading the current centers into an instance variable
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centersPath, conf)) {
        
        VectorWritable key = new VectorWritable();
        IntWritable value = new IntWritable();

        // reads one line into given variables
        while (reader.next(key, value)) {
            centers.add(key);
        }
    }

  }

  public void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
    
    int iteration = context.getConfiguration().getInt("num.iteration",99);

    // initially empty
    VectorWritable closest = null;

    // so that it's always greater than anyone else
    double closestDistance = Double.MAX_VALUE;

    Vector<Double> newSample = new Vector<Double>();

    for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
        double feature = Double.parseDouble(entry.getValue().toString());
        newSample.add(feature);
    }

    // which cluster is closest to this sample? 
    for(VectorWritable c: centers){

        double d = Utils.measureDistance(c.getVector(),newSample);

        if(closest == null){
            closest = c;
            closestDistance = d;
        }else{
            // update cluster?
            if(closestDistance > d){
                closest = c;
                closestDistance = d;
            }
        }
    }

    // emit the pair <the_cluster,point_in_the_cluster>
    context.write( new VectorWritable(closest),new VectorWritable(newSample));

  }
}