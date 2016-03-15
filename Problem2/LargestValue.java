
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

public class LargestValue {

	public static class TraningMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] text = value.toString().split("\n");
			for (String txt : text) {
				String[] cards = txt.split(",");
				if (cards.length == 11) {
					String type = cards[10];
					int sum = 0;
					for (int i = 0; i < cards.length - 1; i+=2) {
						int faceVal = Integer.parseInt(cards[i+1]);
						if (faceVal == 1) {
							faceVal = 14;
						}
						sum += faceVal;
					}
					context.write(new IntWritable(sum), new Text("A"));
				}
			}
		}
	}

	public static class TestingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] text = value.toString().split("\n");
			for (String txt : text) {
				String[] cards = txt.split(",");
				if (cards.length == 11) {
					String type = cards[10];
					int sum = 0;
					for (int i = 0; i < cards.length - 1; i+=2) {
						int faceVal = Integer.parseInt(cards[i+1]);
						if (faceVal == 1) {
							faceVal = 14;
						}
						sum += faceVal;
					}
					context.write(new IntWritable(sum), new Text("B"));
				}
			}
		}

	}

	public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		// The largest value hand
		int maxHand = 0;

		// A comma separated string where the first value is
		// the count of largest value hands from the first file
		// and the second value is the count of largest value hands 
		// from the second file.
		String counts;

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int file1Count = 0;
			int file2Count = 0;
			int keyVal = key.get();
			for (Text val : values) {
				String value = val.toString();
				if (value.equals("A")) {
					file1Count += 1;
				}
				else if (value.equals("B")) {
					file2Count += 1;
				}
			}

			if (keyVal > maxHand) {
				maxHand = keyVal;
				counts = file1Count + "," + file2Count;
			}

		}

		// Only emits the max value hand and the count of occurences
		// from each input file for that value hand.
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		    context.write(new IntWritable(maxHand), new Text(counts));
		}

	}

	public static class CountSort extends WritableComparator {
        protected CountSort() {
            super (IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int j1, int k1, byte[] b2, int j2, int k2) {
            Integer a = ByteBuffer.wrap(b1, j1, k1).getInt();
            Integer b = ByteBuffer.wrap(b2, j2, k2).getInt();
            return a.compareTo(b) * -1;
        }
    }

    public static class FormatMapper extends Mapper<LongWritable, Text, Text, Text> {

    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String file1Name = "poker-hand-traning.true.data.txt";
    		String file2Name = "poker-hand-testing.data.txt";

    		String valueStr = value.toString();
    		String[] vals = valueStr.split("\t");
    		if (vals.length == 2) {
    			String largestVal = vals[0];
    			String fileCountsStr = vals[1];

    			String[] fileCounts = fileCountsStr.split(",");
    			if (fileCounts.length == 2) {
    				int file1Count = Integer.parseInt(fileCounts[0]);
    				int file2Count = Integer.parseInt(fileCounts[1]);

    				String winnerKey = "Contains more hands of the highest value:";
    				if (file1Count > file2Count) {
    					context.write(new Text(winnerKey), new Text(file1Name));
    				}
    				else if (file2Count > file1Count) {
    					context.write(new Text(winnerKey), new Text(file2Name));
    				}
    				else {
    					context.write(new Text(winnerKey), new Text(file1Name + " and " + file2Name));
    				}

    				context.write(new Text("Highest Value Hand:"), new Text(largestVal));

    				context.write(new Text(file1Name + " highest value hands count:"), new Text(fileCounts[0]));
    				context.write(new Text(file2Name + " highest value hands count:"), new Text(fileCounts[1]));

    			}
    		}
    	}

    }

    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {

    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		for (Text val : values) {
    			context.write(key, val);
    		}
    	}

    }

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(LargestValue.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TraningMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestingMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(CountSort.class);

		job.setJobName("Multi file hand sums");
		job.waitForCompletion(true);

		Job formatJob = Job.getInstance();
		formatJob.setJarByClass(LargestValue.class);

		formatJob.setMapperClass(FormatMapper.class);
		formatJob.setReducerClass(FormatReducer.class);

		formatJob.setOutputKeyClass(Text.class);
		formatJob.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(formatJob, new Path(args[2]));
		TextOutputFormat.setOutputPath(formatJob, new Path(args[3]));

		formatJob.waitForCompletion(true);

	}

}
