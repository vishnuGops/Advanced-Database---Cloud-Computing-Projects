//Vishnu Gopal Rajan (1001755911) 

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.util.*;

class Elem implements Writable {
	short tag;
	int index;
	double value;
	
	Elem() {}
	
	Elem (short tag1, int index1 , double value1) {
		this.tag = tag1;
		this.index = index1;
		this.value = value1;
	}
	
	public void write (DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);
	}
	
	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
	}
	public String toString(){
		return tag+" "+index+" "+value;
	}
	

}

class Pair implements WritableComparable<Pair> {
	int i;
	int j;
	
	Pair(){}
	
	Pair (int x , int y) {
		this.i = x;
		this.j = y;
	}
	public void write(DataOutput out) throws IOException{
		out.writeInt(i);
		out.writeInt(j);
	}
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		j= in.readInt();
	}
	
	public int compareTo(Pair temp) {
		if (i> temp.i) {
			return 1;
		}
		else if(i<temp.i) {
			return -1;
		}
		else if(j> temp.j) {
			return 1;
		}
		else if(j< temp.j) {
			return -1;
		}
		return 0;
	}
	
	public String toString(){
		return i+"  "+j;
	}
}
		 
	 


public class Multiply {
	
	
	public static class Mapper1 extends Mapper<Object, Text, IntWritable, Elem>{
		public void map(Object Key, Text value, Context context) throws IOException , InterruptedException {
			Scanner scan = new Scanner(value.toString()).useDelimiter(",");
			int i = scan.nextInt();
			int j = scan.nextInt();
			double v = scan.nextDouble();
			short tag = 0;
			
			Elem element = new Elem(tag,i,v);
			
			context.write(new IntWritable(j), element);
			scan.close();
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, IntWritable, Elem>{
		public void map(Object Key, Text value, Context context) throws IOException , InterruptedException {
			Scanner scan = new Scanner(value.toString()).useDelimiter(",");
			int i = scan.nextInt();
			int j = scan.nextInt();
			double v = scan.nextDouble();
			short tag = 1;
			
			Elem element = new Elem(tag,j,v);
			
			context.write(new IntWritable(i), element);
			scan.close();
		}
	}
			
	

	public static class Reducer1 extends Reducer<IntWritable, Elem, Pair, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException{
			Vector<Elem> A = new Vector<Elem>();
			Vector<Elem> B = new Vector<Elem>();
			
			for(Elem v: values)
			{
				if(v.tag == 0)
				{
					A.add(new Elem(v.tag, v.index, v.value));
				}
				else if(v.tag == 1)
				{
					B.add(new Elem(v.tag, v.index, v.value));
				}
			}
			
			for(Elem a : A)
			{
				for(Elem b: B)
				{
					double prod = a.value * b.value;
					context.write(new Pair(a.index , b.index), new DoubleWritable(prod));
				}
			}
		}
	}

	
	public static class Mapper3 extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable>{
		public void map(Pair key, DoubleWritable value, Context context) throws IOException, InterruptedException{
			context.write(key, value);
		}
	}
	
	public static class Reducer2 extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable>{
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			
			double m = 0.0;
			for(DoubleWritable v : values){
				m += v.get();
			}
			context.write(key, new DoubleWritable(m));
		}
	}
	

    public static void main ( String[] args ) throws Exception {
		
    	Job job1 = Job.getInstance();
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);       
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);      
        job1.setMapperClass(Mapper1.class);
        job1.setMapperClass(Mapper2.class);
        job1.setReducerClass(Reducer1.class);   
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,Mapper1.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,Mapper2.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
        
        
    	Job job2 = Job.getInstance();
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(Mapper3.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
		
		
    }
}
