// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7

// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import java.util.Map.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KitchenUse extends Configured implements Tool {

    public static class DayMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                  String[] splitLine = value.toString().split(";");
                  //Check if kitchen time was recorded
                  if(splitLine.length >= 7) {
                    //key date, time and kitchen power as value
                    context.write(new Text(splitLine[0]), new Text(splitLine[1] + ";" + splitLine[6]));
                  } 
        }
    }

    public static class DayReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {
          HashMap<String, Double> highestDiff = new HashMap<String, Double>();
          HashMap<String, Double> firstFive = new HashMap<String, Double>();
          Double minimum = null;
          String minTime = "";
          String[] timeAndPower;
          String[] timeComps;
          String prevTime;
          Double curPower;
          Double prevPower;
          Double diffPower;
          Integer hour = 0;
          Integer minute = 0;
          Integer prevMinute = 0;
          Integer prevHour = 0;
          for(Text line : values) {
            timeAndPower = line.toString().split(";");
            //Check for question marks
            if(!timeAndPower[0].equals("?") && !timeAndPower[1].equals("?")) {
              //Place the first five into the hash
              if(firstFive.size() != 5 && highestDiff.isEmpty()) {
                firstFive.put(timeAndPower[0], Double.parseDouble(timeAndPower[1]));
              }

              else {
                timeComps = timeAndPower[0].split(":");
                //parse hour and minute
                hour = Integer.parseInt(timeComps[0]);
                minute = Integer.parseInt(timeComps[1]);
                prevMinute = minute - 5;
                //Account for going back to previous hour
                if(prevMinute < 0) {
                  prevMinute += 60;
                  prevHour = hour - 1;
                  //Account for going back 12 hours
                  if(prevHour < 0) {
                    prevHour = 23;
                  }
                }
                prevTime = prevHour + ":" + prevMinute + ":00";
                curPower = Double.parseDouble(timeAndPower[1]);
                //Check first five entries first
                if(!firstFive.isEmpty() && firstFive.containsKey(prevTime)) {
                  prevPower = firstFive.get(prevTime);
                  diffPower = curPower - prevPower;
                  firstFive.remove(prevTime);
                  if(minimum == null || diffPower < minimum) {
                    minimum = diffPower;
                    minTime = timeAndPower[0];
                  }
                }
                else if(highestDiff.size() == 20 && highestDiff.containsKey(prevTime)) {
                  prevPower = highestDiff.get(prevTime);
                  diffPower = curPower - prevPower;
                  //This goes in the top 20
                  if(diffPower > minimum) {
                    highestDiff.put(timeAndPower[0], diffPower);
                    highestDiff.remove(minTime);
                    Entry<String, Double> min = null;
                    for (Entry<String, Double> entry : highestDiff.entrySet()) {
                        if (min == null || min.getValue() > entry.getValue()) {
                            min = entry;
                        }
                    }
                    minimum = min.getValue();
                    minTime = min.getKey();
                  }
                }
                else {
                  prevPower = highestDiff.get(prevTime);
                  diffPower = curPower - prevPower;
                }
                highestDiff.put(timeAndPower[0], diffPower);
              }
            }
          }
          for(Entry<String, Double> entry : highestDiff.entrySet()) {
            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
          }
        }
    }

    public static class MaxMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

        }
    }

    public static class MaxReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "day-job-jtang");

        job.setJarByClass(KitchenUse.class);
        job.setMapperClass(DayMapper.class);
        job.setReducerClass(DayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./test/l8p8inter"));

        job.waitForCompletion(true);

        Job userJob = Job.getInstance(conf, "max-job-jtang");
        userJob.setJarByClass(KitchenUse.class);
        userJob.setMapperClass(MaxMapper.class);
        userJob.setReducerClass(MaxReducer.class);
        userJob.setOutputKeyClass(Text.class);
        userJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(userJob, new Path("./test/l8p8inter", 
                                     "part-r-00000"));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1]));

        return userJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new KitchenUse(), args);
        System.exit(res);
    }
}