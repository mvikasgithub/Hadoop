import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class VotersInXYears 
{
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");                                     
			String strYearsToAdd=con.getConfiguration().get("year");
			int yearsToAdd=Integer.parseInt(strYearsToAdd);
			int age = Integer.parseInt(arr[0]);       
			con.write(new Text("Total Voters in " + yearsToAdd + " Years:"), new IntWritable(age+yearsToAdd));
		}
	}

	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context con) throws IOException, InterruptedException
		{
			String strYearsToAdd=con.getConfiguration().get("year");
			int yearsToAdd=Integer.parseInt(strYearsToAdd);			
			
			int count=0;
			for(IntWritable a:value)
			{			 
				if(a.get()>18 && a.get() - yearsToAdd < 18)
				{
					count++;	
				}
				
			}
			con.write(key,new IntWritable(count));
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
		Configuration cfg=new Configuration();
		Scanner s=new Scanner(System.in);
		System.out.println("Enter years in which you need to know total senior citizens");
        String year=s.next();
        cfg.set("year", year);
		Job job =Job.getInstance(cfg,"VotersInXYears");
	 	job.setJarByClass(VotersInXYears.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(cfg).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
