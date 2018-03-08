import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)
        throws IOException, InterruptedException {
    	Node M = new Node(key.get());
    	double s = 0.0;        
        for (NodeOrDouble value : values) {
            if (value.isNode()){
//then, value is the key of a node.也就是我们这里刚建立的M的node id。 则借此机会把M应该具有的neighbor list给他补充上。         
                M.setOutgoing(value.getNode().outgoing);
            }else{
                s = s + value.getDouble();
            }
        }
        M.pageRank = s;
        context.write(new IntWritable(M.nodeid), M); 
    }
}
