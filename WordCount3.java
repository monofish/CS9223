import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class WordCount3 {

    private static final int TOP_K = 100;

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                int counter = 0;
                char[] tmp = word.toString().toCharArray();

                for(int i = 0; i < word.toString().length(); i++) {
                    if((tmp[i] <= 'z' && tmp[i] >= 'a') || (tmp[i] <= 'Z' && tmp[i] >= 'A'))
                        counter++;
                }

                if(counter == 7)
                    context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private class Pair {
            public String str;
            public Integer cntr;
            public Pair(String str, Integer cntr) {
                this.str = str;
                this.cntr = cntr;
            }
        };

        private PriorityQueue<Pair> queue;

        @Override
        protected void setup(Context ctx) {
            queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
                public int compare(Pair p1, Pair p2) {
                    return p1.cntr.compareTo(p2.cntr);
                }
            });
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            queue.add(new Pair(key.toString(), sum));

            if(queue.size() > TOP_K) {
                queue.remove();
            }
        }

        @Override
        protected void cleanup(Context ctx)
        throws IOException, InterruptedException {
            List<Pair> topKPairs = new ArrayList<Pair>();

            while (! queue.isEmpty()) {
                topKPairs.add(queue.remove());
            }

            for (int i = topKPairs.size() - 1; i >= 0; i--) {
                Pair topKPair = topKPairs.get(i);
                ctx.write(new Text(topKPair.str), 
                    new IntWritable(topKPair.cntr));
            }
        }
    }

    public static class Map2 extends Mapper<Text, Text, Text, IntWritable> {

        @Override
        protected void map(Text key, Text value, Context ctx)

        throws IOException, InterruptedException {
            ctx.write(key, new IntWritable(Integer.valueOf(value.toString())));
        }
    }

    public static void main(String[] args) throws Exception {

        Path temp = new Path("temp0");

        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "wordcount-top-100-pass-1");

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(Map1.class);
        job1.setCombinerClass(Reduce.class);
        job1.setReducerClass(Reduce.class);

        job1.setInputFormatClass(TextInputFormat.class);

        //job1.setNumReduceTasks(1);
        job1.setJarByClass(WordCount3.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, temp);

        boolean succ = job1.waitForCompletion(true);

        if(!succ) {
            System.out.println("Job1 failed, exiting");
        }


        Job job2 = new Job(conf, "wordcount-top-100-pass-2");
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setNumReduceTasks(1);
        job2.setJarByClass(WordCount3.class);

        FileInputFormat.setInputPaths(job2, temp);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        succ = job2.waitForCompletion(true);

        if(!succ) {
            System.out.println("Job2 failed, exiting");
        }
    }
}