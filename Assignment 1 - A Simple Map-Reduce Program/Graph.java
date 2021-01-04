
//Vishnu Gopal Rajan (1001755911)

import java.io.*;
import java.io.FileWriter;
import java.util.Scanner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Graph {

    public static class Mapper1 extends Mapper<Object,Text,LongWritable,LongWritable> {
	@Override
	public void map ( Object key, Text value, Context context) throws IOException, InterruptedException { 
         
	 String line = value.toString(); 
         Scanner check = new Scanner(line).useDelimiter(",");
	 long key2 = check.nextLong();
	 long value2 = check.nextLong();
         context.write( new LongWritable(key2) , new LongWritable(value2)); 
	 check.close();
      } 
   }

   public static class Mapper2 extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable> {
	@Override
	public void map ( LongWritable node, LongWritable count, Context context) throws IOException, InterruptedException { 
         
	 long val = 1;
         context.write(count,new LongWritable(val));

      } 
   }

    public static class Reducer1 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> nodes, Context context) throws IOException, InterruptedException {

	 int count = 0;
	 for(LongWritable i : nodes) {
		count++;
	}
	 context.write( key , new LongWritable(count));
      }
   }
    
 

    public static class Reducer2 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
	 int sum = 0;
	 for(LongWritable v : values) {
		sum+= v.get();	
	}
	 context.write(key , new LongWritable(sum));
       }
    }


    public static void main ( String[] args ) throws Exception {
	String arg0 = args[0];
        String arg1 = args[1];
	//first map reduce job
	Job job1 = Job.getInstance();
        job1.setJobName("FirstJob");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(arg0));
        FileOutputFormat.setOutputPath(job1,new Path("./out"));
        job1.waitForCompletion(true);
	
	//second map reduce job
	Job job2 = Job.getInstance();
        job2.setJobName("SecondJob");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("./out"));
        FileOutputFormat.setOutputPath(job2,new Path(arg1));
        job2.waitForCompletion(true);
		
	
   }
}

