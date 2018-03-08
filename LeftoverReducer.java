import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
        double leftover = (double)Long.parseLong(context.getConfiguration().get("leftover")) / Math.pow(10, 5);
        double size = (double)Long.parseLong(context.getConfiguration().get("size")) / Math.pow(10, 5);    
        for (Node node : Ns){
            node.pageRank = alpha * (1 / size) + (1 - alpha) * (leftover / size + node.getPageRank());
            context.write(nid, node);
        }
    }
}
