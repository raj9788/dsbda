package WordCountPackage;
	
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount{
    public static void main(String [] args) throws Exception{
                Configuration c = new Configuration();
                String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
                Path input=new Path(files[0]);
                Path output=new Path(files[1]);
                Job j=new Job(c,"WordCount");
                j.setJarByClass(WordCount.class);
                j.setMapperClass(MapForWordCount.class);
                j.setReducerClass(ReduceForWordCount.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(j, input);
                FileOutputFormat.setOutputPath(j, output);
                System.exit(j.waitForCompletion(true)?0:1);
        }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        	String line = value.toString();
        	String words [] = line.split(",");
        	for(String word:words){
        		Text outputKey = new Text(word.toUpperCase().trim());
        		IntWritable outputValue = new IntWritable(1);
        		con.write(outputKey,outputValue);
        	}
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

        	int sum=0;
        	for(IntWritable value:values){
        		sum+=value.get();
        	}
        	con.write(key, new IntWritable(sum));

        }

    }
}

/*

[cloudera@quickstart ~]$ cd workspace/WordCountProgram/
[cloudera@quickstart WordCountProgram]$ clear
[cloudera@quickstart WordCountProgram]$ hadoop fs -put wordcountfile WordCountFile
[cloudera@quickstart WordCountProgram]$ hadoop jar WordCount.jar WordCountPackage.WordCount WordCountFile WordCountOutput
[cloudera@quickstart WordCountProgram]$ hadoop fd -ls WordCountOutput
Error: Could not find or load main class fd
[cloudera@quickstart WordCountProgram]$ hadoop fs -ls WordCountOutput
Found 2 items
-rw-r--r--   1 cloudera cloudera          0 2023-05-28 08:42 WordCountOutput/_SUCCESS
-rw-r--r--   1 cloudera cloudera         23 2023-05-28 08:42 WordCountOutput/part-r-00000
[cloudera@quickstart WordCountProgram]$ hadoop fs -cat WordCountOutput/part-r-00000
BUS	10
CAR	10
TRAIN	10
*/