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

public class PowerComp extends Configured implements Tool {

    public static class DayMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                  String[] splitLine = value.toString().split(";");
                  //Check if kitchen time was recorded
                  if(value.toString().charAt(0) != 'D' && splitLine.length >= 9 && !splitLine[7].equals("?") && !splitLine[8].equals("?")) {
                    //key date, time and kitchen power as value
                    context.write(new Text(splitLine[0]), new Text(splitLine[7] + ";" + splitLine[8]));
                  } 
        }
    }

    public static class DayReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {
          int laundryPower = 0;
          int heatPower = 0;
          String[] laundryHeat;
          Double laundryTotal = 0.0;
          Double heatTotal = 0.0;
          Double laundry;
          Double heat;
          for(Text value : values) {
            laundryHeat = value.toString().split(";");
            laundry = Double.parseDouble(laundryHeat[0]);
            heat = Double.parseDouble(laundryHeat[1]);
            if(laundry > 0.0) {
              laundryPower++;
            }
            if(heat > 0.0) {
              heatPower++;
            }
            laundryTotal += laundry;
            heatTotal += heat;
          }
          if(laundryPower > 360 && heatPower > 360) {
            context.write(new Text("Active"), new Text(laundryTotal + ";" + heatTotal));
          }
          else {
            context.write(new Text("NotActive"), new Text(laundryTotal + ";" + heatTotal));
          }
        }
    }

    public static class MaxMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                  String[] lineSplit = value.toString().split("\t");
                  context.write(new Text(lineSplit[0]), new Text(lineSplit[1]));
        }
    }

    public static class MaxReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {
          double average = 0.0;
          int count = 0;
          String[] laundryHeat;
          for(Text value : values) {
            laundryHeat = value.toString().split(";");
            average += Double.parseDouble(laundryHeat[0]);
            average += Double.parseDouble(laundryHeat[1]);
            count++;
          }
          average = average / count;
          context.write(new Text(key.toString() + " Days Average: "), new Text(average + ""));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "day-job-jtang");

        job.setJarByClass(PowerComp.class);
        job.setMapperClass(DayMapper.class);
        job.setReducerClass(DayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./test/l8p9inter"));

        job.waitForCompletion(true);

        Job userJob = Job.getInstance(conf, "max-job-jtang");
        userJob.setJarByClass(PowerComp.class);
        userJob.setMapperClass(MaxMapper.class);
        userJob.setReducerClass(MaxReducer.class);
        userJob.setOutputKeyClass(Text.class);
        userJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(userJob, new Path("./test/l8p9inter", 
                                     "part-r-00000"));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1]));

        return userJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PowerComp(), args);
        System.exit(res);
    }
}