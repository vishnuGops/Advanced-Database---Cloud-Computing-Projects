//Vishnu Gopal Rajan
//1001755911

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                 // the vertex ID
    public Vector<Long> adjacent;   // all vertices adjacent to ID
    public long centroid;           // the id of the centroid in which this vertex belongs to
    public short depth;				// BFS Depth
    public long size; 				// number of abjacent vertices

    public Vertex()
    {}

    public Vertex(long id, Vector<Long> adjacent, long centroid, short depth)
    {
        this.id = id ; 
        this.adjacent = adjacent; 
        this.centroid = centroid;
        this.depth = depth; 
        size = adjacent.size();  
    } 

    //@Override
    public void readFields(DataInput in) throws IOException
    {
        id = in.readLong();
        centroid = in.readLong();
        adjacent = new Vector<Long>(); 
        depth = in.readShort();
        size = in.readLong();
        for(int i = 0; i<size; i++)
        {
            adjacent.add(in.readLong());
        }
    }
    //@Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(id);
        out.writeLong(centroid);
        out.writeShort(depth);
        out.writeLong(size);
        for(int i = 0 ; i<size; i++)
        {
            out.writeLong(adjacent.get(i));
        }       
}
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

public static class Mapper01 extends Mapper<Object, Text, LongWritable, Vertex>
{ 
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] line_split = line.split(",");
		
        long id = Long.parseLong(line_split[0]); 
        long centroid; 
        Vector<Long> adjacent = new Vector<Long>(); 
        short a = 0;
		
        for(int i = 1; i <line_split.length; i++)
        {
            adjacent.add(Long.parseLong(line_split[i]));
        }

	if(centroids.size() < 10)
        {
            centroids.add(id);
            centroid = id;
        }
        else
        {
            centroid = -1; 
	    centroids.add(centroid);
        }
		
        LongWritable l = new LongWritable(id);
        context.write(l, new Vertex(id, adjacent, centroid, a));
}
}

public static class Mapper02 extends Mapper<LongWritable,Vertex, LongWritable, Vertex>{
        
        public void map(LongWritable key, Vertex val, Context context) throws IOException,InterruptedException{
        context.write(new LongWritable(val.id), val);
       
            if (val.centroid > 0)
            {
                for (Long n : val.adjacent)
                {
                    context.write(new LongWritable(n), new Vertex(n, new Vector<Long>(), val.centroid, BFS_depth));
                }
            }
        }
    }

public static class Reducer02 extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{

    @Override
    public void reduce(LongWritable key, Iterable<Vertex> val, Context context) throws IOException,InterruptedException
    {

        short min_depth = 1000;
		long b = -1;
        Vertex m = new Vertex(key.get(),new Vector<Long>(), b ,(short)(0));
        for(Vertex v : val)
        {
            if(!(v.adjacent.isEmpty()))
            {
                m.adjacent = v.adjacent;
            }
            if(v.centroid > 0 && v.depth < min_depth)
            {
                min_depth = v.depth;
                m.centroid = v.centroid;
            }


        }
        m.depth = min_depth;
        context.write(key, m);
    }
}

public static class Mapper03 extends Mapper<LongWritable,Vertex, LongWritable, IntWritable>{
        @Override 
        public void map(LongWritable centroid, Vertex val, Context context) throws IOException,InterruptedException
        {
            context.write(new LongWritable(val.centroid) ,new IntWritable(1));
        }
}

public static class Reducer03 extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable>{

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> val, Context context) throws IOException,InterruptedException
    {
        long m = 0;
        for (IntWritable i : val)
        {
            m = m + Long.valueOf(i.get()) ;
        }
		
        context.write(key, new LongWritable(m));


}
}


    public static void main ( String[] args ) throws Exception {
		//job1
        Job job = Job.getInstance();
        job.setJarByClass(GraphPartition.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper01.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));

        job.waitForCompletion(true);

        for (short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJarByClass(GraphPartition.class);
        
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
        
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
        
            job.setMapperClass(Mapper02.class);
            job.setReducerClass(Reducer02.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
            MultipleInputs.addInputPath(job,new Path(args[1]+"/i"+i),SequenceFileInputFormat.class,Mapper02.class);
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }

        job = Job.getInstance();
        job.setJarByClass(GraphPartition.class);
        job.setReducerClass(Reducer03.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
    
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        MultipleInputs.addInputPath(job,new Path(args[1]+"/i8"),SequenceFileInputFormat.class,Mapper03.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}