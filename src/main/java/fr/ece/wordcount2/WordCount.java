package fr.ece.wordcount2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{

	public int run(String[] args) throws Exception {
	  JobConf conf = new JobConf(getConf(), WordCount.class);
	  conf.setJobName("wordcount");
	
	  conf.setOutputKeyClass(Text.class);
	  conf.setOutputValueClass(IntWritable.class);
	
	  conf.setMapperClass(Map.class);
	  conf.setCombinerClass(Reduce.class);
	  conf.setReducerClass(Reduce.class);
	
	  conf.setInputFormat(TextInputFormat.class);
	  conf.setOutputFormat(TextOutputFormat.class);
	
	  List<String> other_args = new ArrayList<String>();
	  for (int i=0; i < args.length; ++i) {
	    if ("-skip".equals(args[i])) {
       DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
	       conf.setBoolean("wordcount.skip.patterns", true);
	     } else {
	       other_args.add(args[i]);
	     }
	   }

    String basePath = other_args.get(0);
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
  
	   FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
	
	   JobClient.runJob(conf);
	   
	   return 0;
	}
 
	public static void main (String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
	
}
