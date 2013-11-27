package fr.ece.wordcount1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    
    String basePath = args[0];
    List<Path> inputhPaths = new ArrayList<Path>();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] listStatus = fs.globStatus(new Path(basePath + "/*"));
    for (FileStatus fstat : listStatus) {
    	if(fstat.isDir()){
    		// todo: recursivity
    	}else{
    		inputhPaths.add(fstat.getPath());
    	}
    }
    FileInputFormat.setInputPaths(conf,
            (Path[]) inputhPaths.toArray(new Path[inputhPaths.size()]));
    
//    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }

}
