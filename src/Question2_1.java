import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.hadoop.mapreduce.Job;

public class Question2_1 {

	public static class FlickrMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] FlickrPhoto = value.toString().split("\t");
			
			String longitude= FlickrPhoto[10];
			String latitude	= FlickrPhoto[11];
			
			Country c = null;
			if ( latitude != null && latitude != "" && longitude != null && longitude != "" )
				c = Country.getCountryAt(Double.parseDouble(latitude), Double.parseDouble(longitude));
	
			if (c != null) {
				String[] tags = (URLDecoder.decode(FlickrPhoto[8],"UTF-8")+","+ URLDecoder.decode(FlickrPhoto[9],"UTF-8")).split(",");
				
				for (String t : tags) {
					context.write(new Text(c.toString()), new Text(t));
				}
			}			
		}
	}

	public static class FlickrReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> hmap = new HashMap<String, Integer>();
			for (Text value : values) {
				if (hmap.containsKey(value.toString()))
					hmap.put(value.toString(), hmap.get(value.toString()) + 1);
				else
					hmap.put(value.toString(), 1);
			}
			
			MinMaxPriorityQueue<StringAndInt> tagsPopulaires = MinMaxPriorityQueue.maximumSize(context.getConfiguration().getInt("k", 1)).create();
						
			for (Entry<String, Integer> entry : hmap.entrySet()) {
				tagsPopulaires.add(new StringAndInt(entry.getKey(),entry.getValue()));			
			}
			for (StringAndInt stringAndInt : tagsPopulaires) {
				context.write(key, new Text(stringAndInt.getTag() + " " + stringAndInt.getNbrOccurance()));
			}
		}
	}

	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Question2_1");
		conf.setInt("k", Integer.parseInt(otherArgs[2]));
		
	
		job.setJarByClass(Question2_1.class);
		job.setMapperClass(FlickrMapper.class);	
		job.setReducerClass(FlickrReducer.class);
		
		// Types of Key/Value 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Input & Output Files 
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
   
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}