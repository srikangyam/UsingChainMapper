package com.skangyam.hadoop.mapreduce.UsingChainMapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperCalMaxSalForCA extends Mapper<IntWritable, Text, Text, Text> {
	@Override
	protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, Text, Text>.Context context)
	          throws IOException, InterruptedException{
		int sal = key.get();
		String finalOutput = value.toString() + "," + key;
		if ((sal > 100000) && (sal <= 130000)){
			context.write(new Text(finalOutput), new Text());
		}
	}

}
