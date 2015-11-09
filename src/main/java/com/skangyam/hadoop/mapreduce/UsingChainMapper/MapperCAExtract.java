package com.skangyam.hadoop.mapreduce.UsingChainMapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperCAExtract extends Mapper<LongWritable, Text, IntWritable, Text> 
{
	@Override
    protected void map(LongWritable key, Text value, Context context) 
              throws IOException, InterruptedException
    {
		String[] str = value.toString().split(",");
		if (str[1].equalsIgnoreCase("CA")){
			context.write(new IntWritable(Integer.parseInt(str[3].trim())),
					      new Text(str[0]+","+str[1]+","+str[2]));
		}
      
    }
}
