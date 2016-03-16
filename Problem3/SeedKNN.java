// CPE 369 Winter 2016
// Yang Pan, Jordan Tang Lab8

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

public class SeedKNN {

	public static class SeedMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		HashMap<Long, ArrayList<Double>> seedValues = new HashMap<Long, ArrayList<Double>>();
		HashMap<Long, HashMap<Long, Double>> distances = new HashMap<Long, HashMap<Long, Double>>();
		long count = 1;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] values = line.split("\\s+");
			if (values.length == 8) {
				long seed = count++;
				ArrayList<Double> seedVals = new ArrayList<Double>();
				for (int i = 0; i < values.length - 1; i++) {
					seedVals.add(Double.parseDouble(values[i]));
				}

				seedValues.put(seed, seedVals);
			}
		}

		private LinkedHashMap<Long, Double> sortByComparator(Map<Long, Double> unsorted) {
			List<Map.Entry<Long, Double>> list = new LinkedList<Map.Entry<Long, Double>>(unsorted.entrySet());

			Collections.sort(list, new Comparator<Map.Entry<Long, Double>>() {
				public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
					return (o1.getValue()).compareTo(o2.getValue());
				}
			});

			LinkedHashMap<Long, Double> sorted = new LinkedHashMap<Long, Double>();
			for (Iterator<Map.Entry<Long, Double>> it = list.iterator(); it.hasNext();) {
				Map.Entry<Long, Double> entry = it.next();
				sorted.put(entry.getKey(), entry.getValue());
			}
			return sorted;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<Long, ArrayList<Double>> entry : seedValues.entrySet()) {
				long targetKey = entry.getKey();
				ArrayList<Double> targetVals = entry.getValue();
				for (int i = 0; i < seedValues.size(); i++) {
					if (i != (int)targetKey) {
						ArrayList<Double> compareVals = seedValues.get((long)i);
						if (compareVals != null) {
							double cartProd = 0;
							for (int j = 0; j < compareVals.size(); j++) {
								double diff = targetVals.get(j) - compareVals.get(j);
								cartProd += diff * diff;
							}
							double compareDist = Math.sqrt(cartProd);
							long compareSeed = (long)i;

							if (distances.containsKey(targetKey)) {
								HashMap<Long, Double> distPairs = distances.get(targetKey);
								distPairs.put(compareSeed, compareDist);
								distances.put(targetKey, distPairs);
							}
							else {
								HashMap<Long, Double> distPairs = new HashMap<Long, Double>();
								distPairs.put(compareSeed, compareDist);
								distances.put(targetKey, distPairs);
							}
						}
					}
				}
			}

			for (Map.Entry<Long, HashMap<Long, Double>> entry : distances.entrySet()) {
				long outputKey = entry.getKey();
				String outputStr = "";
				HashMap<Long, Double> dists = entry.getValue();
				
				HashMap<Long, Double> sortedDists = sortByComparator(dists);
				int count = 0;
				for (Map.Entry<Long, Double> entr : sortedDists.entrySet()) {
					if (count++ < 5) {
						outputStr += "(" + entr.getKey() + ", " + entr.getValue() + "), ";
					}
				}
				outputStr = outputStr.substring(0, outputStr.length() - 2);
				context.write(new LongWritable(outputKey), new Text(outputStr));
			}
		}
	}

	public static class SeedReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(SeedKNN.class);

		job.setMapperClass(SeedMapper.class);
		job.setReducerClass(SeedReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}