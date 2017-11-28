
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question3_1 {

	public static enum CUSTOM_COUNTER {
		COUNTRY_NOT_FOUND; 
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");

			Country c = Country.getCountryAt(Double.parseDouble(URLDecoder.decode(data[11],"UTF-8")), Double.parseDouble(URLDecoder.decode(data[10],"UTF-8")));
			if (c != null){
				String tags = data[8]+","+data[9];

				for (String t : tags.split(",")){
					context.write(new Text(c.toString()), new StringAndInt(c.toString(),URLDecoder.decode(t,"UTF-8").toString(), 1));
				}
			}
			else
				context.getCounter(CUSTOM_COUNTER.COUNTRY_NOT_FOUND).increment(1);
		}
	}

	public static class MyMapper2 extends Mapper<Text, StringAndInt, StringAndInt, StringAndInt> {
		@Override
		protected void map(Text key, StringAndInt value, Context context) throws IOException, InterruptedException {
			context.write(new StringAndInt(key.toString(), value.tag, value.occ), value);
		}
	}

	public static class StringAndInt implements WritableComparable<StringAndInt> {

		public String country;
		public String tag;
		public int occ;

		public StringAndInt(){}

		public StringAndInt(String country, String tag, int occ) {
			this.country = country;
			this.tag = tag;
			this.occ = occ;
		}

		@Override
		public boolean equals(Object obj) {
			return ((StringAndInt) obj).country.equals(this.country);
		}

		@Override
		public int hashCode() {
			return this.country.hashCode();
		}

		@Override
		public int compareTo(StringAndInt arg0) {
			return this.country.compareTo(arg0.country);
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			country=arg0.readUTF();
			tag=arg0.readUTF();
			occ=arg0.readInt();			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeUTF(country);
			arg0.writeUTF(tag);
			arg0.writeInt(occ);
		}

		@Override
		public String toString() {
			return (country+"-"+tag+"-"+occ);
		}

	}

	public static class MyComparator extends WritableComparator {

		protected MyComparator (){
			super(StringAndInt.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringAndInt k1 = (StringAndInt) a;
			StringAndInt k2 = (StringAndInt) b;

			int comp = k1.compareTo(k2);
			if (comp == 0){
				if (k1.occ < k2.occ)
					return 1;
				else if (k1.occ > k2.occ)
					return -1;
				else
					return k1.tag.compareTo(k2.tag);
			}
			else 
				return comp;
		}

	}

	public static class MyGrouparator extends WritableComparator {

		protected MyGrouparator (){
			super(StringAndInt.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringAndInt k1 = (StringAndInt) a;
			StringAndInt k2 = (StringAndInt) b;
			return k1.compareTo(k2);	
		}

	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (StringAndInt value : values) {
				if (map.containsKey(value.tag))
					map.put(value.tag, map.get(value.tag) + value.occ);
				else
					map.put(value.tag, value.occ);
			}

			for (String k : map.keySet()){
				context.write(key, new StringAndInt(key.toString(), k, map.get(k)));
			}
		}
	}

	public static class MyReducer2 extends Reducer<StringAndInt, StringAndInt, Text, Text> {
		@Override
		protected void reduce(StringAndInt key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			int i=0,K=context.getConfiguration().getInt("K", 1);
			for (StringAndInt value : values) {
				context.write(new Text(key.country), new Text(value.tag + "\t" + value.occ));
				i++;
				if (i == K)
					break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.setInt("K", Integer.parseInt(otherArgs[2]));

		Job job = Job.getInstance(conf, "Question3_1");
		job.setJarByClass(Question3_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringAndInt.class);

		job.setCombinerClass(MyReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Question3_1");
		job2.setJarByClass(Question3_1.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(StringAndInt.class);
		job2.setMapOutputValueClass(StringAndInt.class);
		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setSortComparatorClass(MyComparator.class);
		job2.setGroupingComparatorClass(MyGrouparator.class);

		FileInputFormat.addInputPath(job2, new Path(output));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output+"/final"));
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.waitForCompletion(true);
	}
}