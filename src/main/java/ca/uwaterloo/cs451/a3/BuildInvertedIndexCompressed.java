package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfStringLong;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapreduce.Partitioner;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    // private static final Text WORD = new Text();
    private static final PairOfStringInt WORDDIDPAIR = new PairOfStringInt();
    private static final IntWritable FREQUENCY = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        WORDDIDPAIR.set(e.getLeftElement(),(int)docno.get());
        FREQUENCY.set(e.getRightElement());
        context.write(WORDDIDPAIR, FREQUENCY);
      }
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    // private static final IntWritable TF = new IntWritable();
    private String termPrev = "";
    private ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
    private int pDocon = 0;
    private int df =0;
    private ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    private DataOutputStream outputStream = new DataOutputStream(byteStream);
    private Text term = new Text();
    IntWritable DF = new IntWritable();

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();    
      
      if(!key.getLeftElement().equals(termPrev) && !termPrev.equals("")){
        DF.set(df);
        term.set(termPrev);
        context.write(term,new pairOfWritables(DF,new BytesWritable(byteStream.toByteArray())));
        byteStream.reset();
        outputStream.flush();
        pDocon = 0;
        df = 0;
      }
      int tf = 0;
      while (iter.hasNext()) {
        tf+=iter.next().get();
      }

      df++;
      WritableUtils.writeVInt(outputStream, (key.getRightElement()-pDocon));
      WritableUtils.writeVInt(outputStream, tf);
      pDocon = key.getRightElement();
      termPrev = key.getLeftElement();
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        if(!termPrev.equals("")){
            DF.set(df);
            term.set(termPrev);
            context.write(term,new pairOfWritables(DF,new BytesWritable(byteStream.toByteArray())));
            byteStream.reset();
            outputStream.flush();
            df = 0;
            pDocon = 0;
        }
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
