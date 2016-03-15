// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7

// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


import java.io.IOException;
import java.util.Iterator;

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
import org.json.JSONObject;

public class PowerDays extends Configured implements Tool {

    public static class DayMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                  String line = value.toString();
                  if(line.charAt(0) != 'D') {
                      String date = line.split(";")[0];
                      context.write(new Text(date), new Text(line));
                  }
        }
    }

    public static class DayReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {
            double totalPwr = 0.0;
            String date = "";
            for(Text line : values) {
                String questionCheck = line.toString().split(";")[2];
                if(!questionCheck.equals("?")) {
                  double globalPwr = Double.parseDouble(questionCheck);
                  totalPwr += globalPwr;
                  if(date.isEmpty()) {
                    date = line.toString().split(";")[0];
                  }
                }
            }
            if(!date.isEmpty()) {
              context.write(new Text(date), new DoubleWritable(totalPwr));
            }
        }
    }

    public static class YearMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                  String[] line = value.toString().split("\t");
                  String[] dateCheck = line[0].split("/");
                  if(dateCheck.length == 3 && line.length == 2) {
                    String year = dateCheck[2];
                    context.write(new Text(year), value);
                  }
        }
    }

    public static class YearReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
         Context context) throws IOException, InterruptedException {
            double maxPwr = 0.0;
            String maxDate = "";
            String[] lineSplit;
            for(Text line : values) {
              lineSplit = line.toString().split("\t");
              double curPwr = Double.parseDouble(lineSplit[1]);
              if(maxPwr < curPwr || maxPwr == 0.0) {
                maxPwr = curPwr;
                maxDate = lineSplit[0];
              }         
            }
            if(!maxDate.isEmpty()) {
              context.write(key, new Text(maxDate + "\t" + maxPwr));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "year-job-jtang");

        job.setJarByClass(PowerDays.class);
        job.setMapperClass(DayMapper.class);
        job.setReducerClass(DayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./test/l8p7inter"));

        job.waitForCompletion(true);

        Job userJob = Job.getInstance(conf, "day-job-jtang");
        userJob.setJarByClass(PowerDays.class);
        userJob.setMapperClass(YearMapper.class);
        userJob.setReducerClass(YearReducer.class);
        userJob.setOutputKeyClass(Text.class);
        userJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(userJob, new Path("./test/l8p7inter", 
                                     "part-r-00000"));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1]));

        return userJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PowerDays(), args);
        System.exit(res);
    }
}