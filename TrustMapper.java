import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

//IntWritable is for nodeID, Node is the Node class, NodeOrDouble is 
public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
    	context.getCounter(mycounter.MY_COUNTER.NODE_NUM).increment((long)Math.pow(10, 5));
        if (value.outgoingSize() == 0){
    		context.write(key, new NodeOrDouble(value));
    		context.getCounter(mycounter.MY_COUNTER.DANGLING_MASS).increment((long)(value.getPageRank() * Math.pow(10, 5)));
    	}else{
    		double p = value.getPageRank() / value.outgoingSize();
        	context.write(key, new NodeOrDouble(value));
        	for (int i : value.outgoing){
        		context.write(new IntWritable(i), new NodeOrDouble(p)); 
        	}
    	}
    }
}





public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
        // context.getCounter这句话不太懂啊。是要做什么？
        context.getCounter(mycounter.MY_COUNTER.NODE_NUM).increment((long)Math.pow(10, 5));
            double p = value.getPageRank() / value.outgoingSize();
            context.write(key, new NodeOrDouble(value));
            for (int i : value.outgoing){
                context.write(new IntWritable(i), new NodeOrDouble(p)); 
            }
    }
}

//For the simple reason：handle the Objects in Hadoop way; For example hadoop use Text instead of java String; 
//The Text class in hadoop is similar with java String,while Text class implemented the Interfaces like Comparable ,
// Writable, Writable Comparable; Those Intefaces are all useful for hadoop MapReduce; The Comparable Interface can use 
//for Compare when the reduce sort the keys; and Writable can write the result to the local disk(it not use the java 
//Serializable because java Serializable is too big or too heavy for hadoop, Writable can Serializabled the hadoop 
//Object in a very light way);