package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class TopKPageRankPages extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(TopKPageRankPages.class);

  public static void main(final String[] args) throws Exception {
    ToolRunner.run(new TopKPageRankPages(), args);
  }

  @Override
  public int run(final String[] args) throws Exception {
    int k=100; String inputpath=null, outputdir=null;
    for(int i = 0; i < args.length; i++){
      if("-k"     .equals(args[i])){k        =Integer.parseInt(args[++i]);} else
      if("-input" .equals(args[i])){inputpath=args[++i];}                   else
      if("-output".equals(args[i])){outputdir=args[++i];}
    }
    if(inputpath==null || (inputpath=inputpath.trim()).isEmpty()){throw new Exception("argument '-input' is missing");}
    if(outputdir==null || (outputdir=outputdir.trim()).isEmpty()){throw new Exception("argument '-output' is missing");}

    final Job job = Job.getInstance(getConf(), "Top" + k + "-Pages");
    final JobBuilder builder = JobBuilder.build(job).setJarByClass(TopKPageRankPages.class);
    builder.setInt("k", k);
    builder.inputPaths(new Path(inputpath)).outputPath(new Path(outputdir));
    builder.inputFormatClass(SequenceFileInputFormat.class).outputFormatClass(TextOutputFormat.class);
    builder.mapOutputKeyClass(Text.class).mapOutputValueClass(FloatWritable.class);
    builder.outputKeyClass(Text.class).outputValueClass(FloatWritable.class);
    // builder.compressMapperOutput().compressReducerOutput(!JobBuilder.SEQUENCE_FILE);
    builder.mapperClass(TheMapper.class).reducerClass(TheReducer.class).speculativeExecution(false);
    builder.numReduceTasks(1);
    deleteDirs(job.getConfiguration(), outputdir);

    final long startTime = System.nanoTime();
    final boolean ok = job.waitForCompletion(true);
    log.info("job " + job.getJobID() + "completed in " + elapsed(startTime) + " seconds with status " + ok);

    return ok ? 0 : -1;
  }

  static class PageRankPair {
    PageRankPair(final Text title_, final float rank_) { title = title_; rank = rank_; }
    Text title; float rank;
  }

  static class PagePriorityQueue extends PriorityQueue<PageRankPair> {
    public PagePriorityQueue(final int maxSize) {
      super.initialize(maxSize);
    }

    @Override /** if lhs is less than rhs return true otherwise false */
    protected boolean lessThan(final Object lhs, final Object rhs) {
      return ((PageRankPair) lhs).rank < ((PageRankPair) rhs).rank;
    }

    public void add(final Text title, final float rank) { insert(new PageRankPair(title, rank)); }

    public PageRankPair[] topK() {
      final int len = size();
      final PageRankPair[] arr = new PageRankPair[len];
      for (int i = 0; i < len; i++) {
        arr[len - 1 - i] = pop();
        //
        // put in reverse order as pop returns the least element
        //
        // or we could also reverse the 'lessThan' method so that
        // pop will return in the order we want
        //
      }
      return arr;
    }
  }

  static class TheMapper extends Mapper<Text, Page, Text, FloatWritable> {
    private PagePriorityQueue queue;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      final int k = context.getConfiguration().getInt("k", 100);
      queue = new PagePriorityQueue(k);
    }

    @Override
    protected void map(final Text title, final Page page, final Context context) throws IOException, InterruptedException {
      queue.add(title, page.rank);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
      final Text key = new Text();
      final FloatWritable val = new FloatWritable();
      final PageRankPair[] topK = queue.topK();
      for (int i = 0; i < topK.length; i++) {
        final PageRankPair pair = topK[i];
        key.set(pair.title);
        val.set(pair.rank);
        context.write(key, val);
      }
    }
  }

  static class TheReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private PagePriorityQueue queue;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      final int k = context.getConfiguration().getInt("k", 100);
      queue = new PagePriorityQueue(k);
    }

    @Override
    protected void reduce(final Text title, final Iterable<FloatWritable> ranks, final Context context) throws IOException,
        InterruptedException {
      final Iterator<FloatWritable> iter = ranks.iterator();
      queue.add(title, iter.next().get());
      if (iter.hasNext()) { // error check
        log.error("multiple ranks for the page '" + title + "'");
      }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
      final Text key = new Text();
      final FloatWritable val = new FloatWritable();
      final PageRankPair[] topK = queue.topK();
      for (int i = 0; i < topK.length; i++) {
        final PageRankPair pair = topK[i];
        key.set(pair.title);
        val.set(pair.rank);
        context.write(key, val);
      }
    }
  }
}
