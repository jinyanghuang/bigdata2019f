package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;
import tl.lin.data.pair.PairOfObjectDouble;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static final class MyMapperCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Text KEY = new Text();
    private static final IntWritable ONE = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        List<String> tokens = Tokenizer.tokenize(value.toString());
        ArrayList<String> wordAppear = new ArrayList<String>();
        for (int i = 0; i < tokens.size() && i < 40; i++) {
            String word = tokens.get(i);
            if (!wordAppear.contains(word)) {
                wordAppear.add(word); //check if 1 can be Integer
                KEY.set(word);
                context.write(KEY,ONE);
            }
        }
        KEY.set("*");
        context.write(KEY,ONE);
        
    }
  }

  private static final class MyCombinerCount extends
            Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
            sum += iter.next().get();
        }
        
            SUM.set(sum);
            context.write(key, SUM);
        
        }
    }

  private static final class MyReducerCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
          // Sum up values.
          Iterator<IntWritable> iter = values.iterator();
          int sum = 0;
          while (iter.hasNext()) {
              sum += iter.next().get();
          }
          
              SUM.set(sum);
              context.write(key, SUM);
        } 
  }
  private static final class MyPartitionerCount extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
    

  private static final class MyMapperPMI extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            ArrayList<String> wordAppearOutter = new ArrayList<String>();
            for (int i = 0; i < Math.min(40, tokens.size()); i++) {
                if (wordAppearOutter.contains(tokens.get(i))) continue;
                wordAppearOutter.add(tokens.get(i));
                ArrayList<String> wordAppearInner = new ArrayList<String>();
                wordAppearInner.add(tokens.get(i));
                for (int j = 0; j < Math.min(40, tokens.size()); j++) {
                  if (i == j) continue;
                  if (wordAppearInner.contains(tokens.get(j))) continue;
                  wordAppearInner.add(tokens.get(j));
                  PAIR.set(tokens.get(i), tokens.get(j));
                  context.write(PAIR, ONE);
                }
              }
            
    }
  }

  private static final class MyCombinerPMI extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class MyReducerPMI extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfStrings> {
    private static final PairOfStrings VALUEPAIR = new PairOfStrings();
    private static Map<String,Integer> wordTotal = new HashMap<String,Integer>();
    private static int totalLine;
    private int threshold = 10;

    @Override
    public void setup(Context context) throws IOException{
        
        threshold = context.getConfiguration().getInt("threshold", 10);
        //read file
        Path path = new Path("intermediate/part-r-00000");

        Text key = new Text();
        IntWritable value = new IntWritable();
        SequenceFile.Reader reader =
                    new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));

        while (reader.next(key, value)) {
            if (key.toString().equals("*")) {
                totalLine = value.get();
            } else {
                wordTotal.put(key.toString(), value.get());
            }
        }
        reader.close();
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      float sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if(sum>= threshold && !key.getRightElement().equals("*")){
      float numX = wordTotal.get(key.getLeftElement());
      float numY = wordTotal.get(key.getRightElement());

      double pmi = Math.log10(sum * totalLine/(numX * numY));
      VALUEPAIR.set(String.valueOf(pmi),String.valueOf(sum));

    //   SUM.set(sum);
      context.write(key, VALUEPAIR);
      }
    }
    
  }

  

  private static final class MyPartitionerPMI extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "number of threshold")
    int numThreshold = 10;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }
    // first job 
    LOG.info("Tool: " + PairsPMI.class.getSimpleName() + "count word");
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - number of threshold: " + args.numThreshold);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName(PairsPMI.class.getSimpleName()+"WordCount");
    job1.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    String intermediateDir = "intermediate";
    Path intermediatePath = new Path(intermediateDir);
    FileSystem.get(getConf()).delete(intermediatePath, true);

    job1.getConfiguration().setInt("threshold", args.numThreshold);

    job1.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(intermediateDir));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    //set output format
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(MyMapperCount.class);
    job1.setCombinerClass(MyCombinerCount.class);
    job1.setReducerClass(MyReducerCount.class);
    job1.setPartitionerClass(MyPartitionerCount.class);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // second job
    LOG.info("Tool: " + PairsPMI.class.getSimpleName() + "calculate PMI");
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - number of threshold: " + args.numThreshold);

    Job job2 = Job.getInstance(getConf());
    job2.setJobName(PairsPMI.class.getSimpleName()+"PMICalculation");
    job2.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job2.getConfiguration().setInt("threshold", args.numThreshold);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfStrings.class);
    //set output format
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyMapperPMI.class);
    job2.setCombinerClass(MyCombinerPMI.class);
    job2.setReducerClass(MyReducerPMI.class);
    job2.setPartitionerClass(MyPartitionerPMI.class);

//     long startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    FileSystem.get(getConf()).delete(intermediatePath, true);
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
