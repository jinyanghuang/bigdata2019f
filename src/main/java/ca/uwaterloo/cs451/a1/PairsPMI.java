package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.FloatWritable;


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static Map<String,Integer> wordTotal = new HashMap<String,Integer>();

    // Mapper: emits (token, 1) for every word occurrence.
    // first MapReduce counts the occurrences of all words.
  public static final class MyMapperCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        List<String> tokens = Tokenizer.tokenize(value.toString());
        for (int i = 0; i < tokens.size(); i++) {
            for (int j = 0; j < Math.min(40, tokens.size()); j++) {
              if (i == j) continue;
              PAIR.set(tokens.get(i), tokens.get(j));
              context.write(PAIR, ONE);
              PAIR.set(tokens.get(i), "*");
              context.write(PAIR, ONE);
            }
          }
        }
    }

   private static final class MyCombinerCount extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
            sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

  // Reducer: sums up all the occurrences.
  public static final class MyReducerCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();
    private static final PairOfStrings PAIR = new Text();
    private int totalCount = 0;

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if (key.getRightElement().equals("*")){
        totalCount += sum;
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        SUM.set(totalCount);
        PAIR.set("*","*");
        context.write(PAIR, SUM);
    }
  }

    

//   private static final class MyMapperPMI extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
//     private static final PairOfStrings PAIR = new PairOfStrings();
//     private static final IntWritable ONE = new IntWritable(1);
    

//     @Override
//     public void map(PairOfStrings key, IntWritable value, Context context)
//         throws IOException, InterruptedException {
//     //   List<String> tokens = Tokenizer.tokenize(value.toString());

//     //   for (int i = 0; i < tokens.size(); i++) {
//     //     for (int j = 0; j < Math.min(40, tokens.size()); j++) {
//     //       if (i == j) continue;
//     //       PAIR.set(tokens.get(i), tokens.get(j));
//     //       context.write(PAIR, ONE);
//     //       PAIR.set(tokens.get(i), "*");
//     //       context.write(PAIR, ONE);
//     //     }
//     //   }
//         if (key.getRightElement().equals("*")){
//             wordTotal.put(key.getLeftElement(), Integer.parseInt(value));
//         }

//         context.write(key, value);

//     }
//   }

// //   private static final class MyCombinerPMI extends
// //       Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
// //     private static final FloatWritable SUM = new FloatWritable();

// //     @Override
// //     public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
// //         throws IOException, InterruptedException {
// //       int sum = 0;
// //       Iterator<FloatWritable> iter = values.iterator();
// //       while (iter.hasNext()) {
// //         sum += iter.next().get();
// //       }
// //       SUM.set(sum);
// //       context.write(key, SUM);
// //     }
// //   }

//   private static final class MyReducerPMI extends
//       Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
//     private static final IntWritable SUM = new IntWritable();
//     private static final PairOfFloat VALUEPAIR = new PairOfFloat();
//     private static final FloatWritable PMI = new FloatWritable();
//     private static int totalAppear;

//     @Override
//     public void setup(Context context) {
//         totalAppear = wordTotal.get("*");
//     }

//     @Override
//     public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
//         throws IOException, InterruptedException {
//       Iterator<IntWritable> iter = values.iterator();
//       float sum = 0.0f;
//       while (iter.hasNext()) {
//         sum += iter.next().get();
//       }

//       float probPair = sum / totalAppear;
//       float probX = wordTotal.get(key.getLeftElement()) / totalAppear;
//       float probY = wordTotal.get(key.getRightElement()) / totalAppear;

//       float pmi = Math.log(probPair/(probX * probY));
//       SUM.set(sum);
//       PMI.set(pmi);
//       VALUEPAIR.set(SUM,PMI);

//     //   SUM.set(sum);
//       context.write(key, VALUEPAIR);
//     }
//   }

  

//   private static final class MyPartitionerPMI extends Partitioner<PairOfStrings, IntWritable> {
//     @Override
//     public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
//       return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
//     }
//   }

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

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
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
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName(PairsPMI.class.getSimpleName()+"WordCount");
    job1.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    Path intermediatePath = new Path("intermediate results");
    FileSystem.get(getConf()).delete(outputDir, true);

    job1.getConfiguration().setInt("window", args.window);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    //set output format
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapperCount.class);
    job1.setCombinerClass(MyCombinerCount.class);
    job1.setReducerClass(MyReducerCount.class);

    long startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

//     // second job
//     LOG.info("Tool: " + PairsPMI.class.getSimpleName() + "calculate PMI");
//     LOG.info(" - input path: " + args.input);
//     LOG.info(" - output path: " + args.output);
//     LOG.info(" - window: " + args.window);
//     LOG.info(" - number of reducers: " + args.numReducers);

//     Job job2 = Job.getInstance(getConf());
//     job2.setJobName(PairsPMI.class.getSimpleName()+"PMICalculation");
//     job2.setJarByClass(PairsPMI.class);

//     // Delete the output directory if it exists already.
//     Path outputDir = new Path(args.output);
//     FileSystem.get(getConf()).delete(outputDir, true);

//     job2.getConfiguration().setInt("window", args.window);

//     job2.setNumReduceTasks(args.numReducers);

//     FileInputFormat.setInputPaths(job2, new Path(intermediatePath));
//     FileOutputFormat.setOutputPath(job2, new Path(args.output));

//     job2.setMapOutputKeyClass(PairOfStrings.class);
//     job2.setMapOutputValueClass(IntWritable.class);
//     job2.setOutputKeyClass(PairOfStrings.class);
//     job2.setOutputValueClass(IntWritable.class);
//     job2.setInputFormatClass(SequenceInputFormat.class);
//     //set output format
//     job2.setOutputFormatClass(TextOutputFormat.class);

//     job2.setMapperClass(MyMapperPMI.class);
//     // job2.setCombinerClass(MyCombinerPMI.class);
//     job2.setReducerClass(MyReducerPMI.class);
//     job2.setPartitionerClass(MyPartitionerPMI.class);

//     long startTime = System.currentTimeMillis();
//     job2.waitForCompletion(true);
//     System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

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
