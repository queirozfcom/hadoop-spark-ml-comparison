/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.queirozf.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.AbstractJob;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

import org.apache.mahout.math.VectorWritable;

public class MahoutKMeans extends AbstractJob{

    public static void main(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println("Args: <inputpath> <outputpath> <k>");
            System.exit(1);
        }

        ToolRunner.run(new Configuration(), new MahoutKMeans() , args);

    }

    @Override
    public int run(String[] args) throws Exception {

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        int k = Integer.parseInt(args[2]);

        double epsilon = 0.001;
        int maxIterations = 10000;

        Configuration conf = this.getConf();

        DistanceMeasure measure = new EuclideanDistanceMeasure();

        Path centroids = RandomSeedGenerator.buildRandom(conf, in, new Path(out, "data/clusters"), k, measure);

        KMeansDriver.run(conf,in,centroids,out,epsilon,maxIterations,true,0.0,false);

        return 0;
    }

}