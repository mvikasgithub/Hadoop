import java.io.IOException;

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


//Citizen vs. Immigrants Ratio for all Employed

public class CitizenVsImmigrantOnEmployed 
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] arr = value.toString().split(",");
				
			String weeksWorked = arr[9];
			
			if(weeksWorked != "0")
				context.write(new Text(weeksWorked), new Text(arr[8]));
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			int ccount= 0, ncount = 0; //citizencount - noncitizencount
			
			for(Text i: value)
			{
				if(i.toString().trim().contains("Native") || i.toString().trim().contains("naturalization"))
					ccount++;
				else
					ncount++;			}

			String out = String.valueOf(ccount) + " : " + String.valueOf(ncount);
			context.write(new Text(key), new Text(out));
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
		Job job = Job.getInstance(cfg, "CitizenVsImmigrationOnEmployed");
		job.setJarByClass(CitizenVsImmigrantOnEmployed.class);
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
