import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TotalTxnPerCustomer 
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] arr = value.toString().split(",");
					
			context.write(new Text(arr[2]), new FloatWritable(Float.parseFloat(arr[3])));
		}
	}
	
	public static class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException
		{
			float sum = 0.0f;
			for(FloatWritable val:value)
			{
				sum += val.get();
			}
			context.write(key, new FloatWritable(sum));
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
		// TODO Auto-generated method stub
		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg, "TotalTxnPerCustomer");
		job.setJarByClass(TotalTxnPerCustomer.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(cfg).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);			

	}

}
