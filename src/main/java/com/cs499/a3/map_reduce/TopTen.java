package com.cs499.a3.map_reduce;

import java.util.List;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopTen extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TopTen(), args);
        List<Integer> movieIds = new ArrayList<>();
        TreeMap<Integer, String> movieTitles = new TreeMap<>();
        TreeMap<Double, Integer> movieRatings = new TreeMap<>(new MovieComparator());
        TreeMap<Integer, Integer> userRanking = new TreeMap<>(new MovieComparator2());
        String fileName1 = "output/part-r-00000", fileName2 = "movie_titles.txt", fileName3 = "output2/part-r-00000";
        
		try (Stream<String> stream1 = Files.lines(Paths.get(fileName1));
			 Stream<String> stream2 = Files.lines(Paths.get(fileName2));
			 Stream<String> stream3 = Files.lines(Paths.get(fileName3))) {
			stream1.forEach(str -> {
				movieRatings.put(Double.parseDouble(str.split("\t+")[1]), Integer.parseInt(str.split("\t+")[0]));
			});
			stream2.forEach(str -> {
				movieTitles.put(Integer.parseInt(str.split("(?<=\\G\\w+),")[0]), str.split("(?<=\\G\\w+),")[2]);
			});
			stream3.forEach(str -> {
				userRanking.put(Integer.parseInt(str.split("\t+")[1]), Integer.parseInt(str.split("\t+")[0]));
			});

			movieRatings.forEach((key, value) -> {
				if (movieIds.size() != 10) movieIds.add(value);
				else return;
			});
		}catch (IOException e) {e.printStackTrace();}
		
		System.out.println("\n***Top Ten Movies***");
		movieIds.forEach(id -> {System.out.println(movieTitles.get(id));});
		
		System.out.println("\n***Top Ten Users***");
		userRanking.forEach((key, value) -> {
			if (movieIds.size() != 20) {
				System.out.println(userRanking.get(key));
				movieIds.add(1);
			}else return;
		});
		
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, " + 
            		"input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(TopTen.class);
        job.setJobName("TopTenMovies");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.waitForCompletion(true);
        
        Job job2 = new Job();
        job2.setJarByClass(TopTen.class);
        job2.setJobName("TopTenUsers");

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path("output2"));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        job2.setMapperClass(MapClass2.class);
        job2.setReducerClass(ReduceClass2.class);
        
        int returnValue = job2.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) System.out.println("Job was successful");
        else if(!job.isSuccessful()) System.out.println("Job was not successful");
        
        return returnValue;
    }

	static class MapClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	    private DoubleWritable ratings = new DoubleWritable();
	    private Text word = new Text();
	
	    protected void map(LongWritable key, Text value, 
	    		Context context) throws IOException, InterruptedException { 
			word.set(value.toString().split(",")[0]);
			ratings.set((Double.parseDouble(String.valueOf(value.toString().split(",")[2].charAt(0)))));
			context.write(word, ratings);
	    }
	}
	
	static class ReduceClass<Key> extends Reducer<Key, DoubleWritable, Key, DoubleWritable>{
	    private DoubleWritable result = new DoubleWritable();
	
	    protected void reduce(Key key, Iterable<DoubleWritable> values,
	                       Context context) throws IOException, InterruptedException {
	        long sum = 0, divisor = 0;
	        for (DoubleWritable val : values) {
	        	sum += val.get();
	        	divisor++;
	        }
	        result.set((sum*1.0)/(divisor*1.0));
	        context.write(key, result);
	    }
	}
	
	static class MapClass2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	
	    protected void map(LongWritable key, Text value, 
	    		Context context) throws IOException, InterruptedException { 
			word.set(value.toString().split(",")[1]);
			context.write(word, one);
	    }
	}
	
	static class ReduceClass2<Key> extends Reducer<Key, IntWritable, Key, IntWritable>{
	    private IntWritable result = new IntWritable();
	
	    protected void reduce(Key key, Iterable<IntWritable> values,
	                       Context context) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {sum += val.get();}
	        result.set(sum);
	        context.write(key, result);
	    }
	}
	
	static class MovieComparator implements Comparator<Double> {
		public int compare(Double rating1, Double rating2) {
	        if (rating1 > rating2) return -1;
	        if (rating2 > rating1) return 1;
	        return 0;
		}
	}
	
	static class MovieComparator2 implements Comparator<Integer> {
		public int compare(Integer rating1, Integer rating2) {
	        if (rating1 > rating2) return -1;
	        if (rating2 > rating1) return 1;
	        return 0;
		}
	}
}


























