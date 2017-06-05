import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TotalTaxToBeCollected 
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] arr = value.toString().split(",");
				
			context.write(new Text(""), new Text(arr[5]));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			float totalTax= 0; 
			
			for(Text i: value)
			{
				float income = Float.parseFloat(i.toString());
				
				if(income > 500 && income < 1000)
					totalTax += income * 0.05;
				else if(income > 1000 && income < 2000)
					totalTax += income * 0.10;
				else if(income > 2000)
					totalTax += income * 0.15;
			}	
			
			String out = String.valueOf(totalTax);
				
			context.write(new Text("Total Tax Due: "), new Text(out));
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
		Job job = Job.getInstance(cfg, "TotalTaxToBeCollected");
		job.setJarByClass(TotalTaxToBeCollected.class);
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
