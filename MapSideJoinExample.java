import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapSideJoinExample 
{
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		HashMap<String, String> userdata = new HashMap <String, String> ();
		Text outkey = new Text();
		Text outvalue = new Text();
		
		
		public void setup(Context context)
		{
			try
			{
				Path files[]= DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for(Path f:files)
				{
					if(f.getName().equals("a1"))
					{
						BufferedReader br = new BufferedReader(new FileReader(f.toString()));
										
						String line;
						while((line = br.readLine()) != null)
						{
							String str[] = line.split(",");
							
							String name = str[0];
							String marks = str[1];
							try{
							userdata.put(name, marks);
							}
							catch(Exception e)
							{
								System.err.println(e);
							}
							
							line = br.readLine();
						} // end of while
						br.close();
						
							
					}// end of if
				}// end of for
			}// end of try
			catch(Exception e)
			{
				System.err.println("File not founc in Distributed Cache");
			}	 
		
		}// end of setup method
	// end of MyMapper`
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String str[] = value.toString().split(","); // name email location
			String name = str[0];
         String marks=userdata.get(name);
			
			String email = str[1];
			String location = str[2];
			
	
			
			outkey.set(name);
			String out = marks +"-" + email +"-" + location;
			outvalue.set(out);
			
			context.write(outkey, outvalue);
			
		}
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		// TODO Auto-generated method stub
		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg, "MapSideJoinExample");
		try{
		DistributedCache.addCacheFile(new URI("/user/hadoop/a1"), cfg);
		}
		catch(Exception e)
		{
			System.err.println(e);
		}
		job.setJarByClass(MapSideJoinExample.class);
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(cfg).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}
}