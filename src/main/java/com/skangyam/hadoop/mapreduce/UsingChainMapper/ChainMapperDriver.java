//Added a dependency of hadoop-mapreduce-client-core to POM 
//for ChainMapper

package com.skangyam.hadoop.mapreduce.UsingChainMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ChainMapperDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input1 dir> <output dir>\n",
					getClass().getSimpleName());
			return -1;
		}
		
		//JobConf conf = new JobConf(getConf(), ChainMapperDriver.class);
        //conf.setJobName(this.getClass().getName());

        //FileInputFormat.setInputPaths(conf, new Path(args[0]));
        //FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
		
		Job job = new Job(getConf());
		
		//Configuration for the two maps invoked via ChainMapper
		
		Configuration confM1 = new Configuration(false);
		Configuration confM2 = new Configuration(false);
		
		ChainMapper.addMapper(job, MapperCAExtract.class, LongWritable.class, Text.class,
				              IntWritable.class, Text.class, confM1);
		
		ChainMapper.addMapper(job, MapperCalMaxSalForCA.class, IntWritable.class, Text.class,
	              Text.class, Text.class, confM2);
		
		job.setJarByClass(ChainMapperDriver.class);
		job.setJobName(this.getClass().getName());
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		if (job.waitForCompletion(true));
		
		//JobClient.runJob(conf);
				
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new ChainMapperDriver(), args);
		System.exit(exitCode);

	}

}
