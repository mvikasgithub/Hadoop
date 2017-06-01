import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestCustIn2015 
{
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] arr = value.toString().split(",");
					
			context.write(new Text(arr[2]), new FloatWritable(Float.parseFloat(arr[3])));
		}
	}

	public static class MyReducer extends Reducer<Text, FloatWritable, Text, Text>
	{
		TreeMap<Float, String> tm = new TreeMap<Float,String>();
		public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException
		{
			float sum = 0.0f;
			for(FloatWritable val:value)
			{
				sum += val.get();
				
			}
			tm.put(sum,  key.toString());
			
			if(tm.size() > 1)
				tm.remove(tm.firstKey());
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text("Best Customer in 2015"), new Text(tm.toString()));
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg, "BestCustIn2015");
		job.setJarByClass(BestCustIn2015.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(cfg).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		

	}

}
