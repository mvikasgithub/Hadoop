import java.io.IOException;


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


public class TotalTaxPayers 
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] arr = value.toString().split(",");
				
			String taxFilerStatus = arr[4];
			
			context.write(new Text(""), new Text(taxFilerStatus));
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			int sum= 0;
			for(Text i: value)
			{
				if(!(i.toString().trim().equalsIgnoreCase("nonfiler")))
					sum++;
			}
			context.write(new Text("Total Tax Payers"), new IntWritable(sum));
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
		Job job = Job.getInstance(cfg, "TotalTaxPayers");
		job.setJarByClass(TotalTaxPayers.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(cfg).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
