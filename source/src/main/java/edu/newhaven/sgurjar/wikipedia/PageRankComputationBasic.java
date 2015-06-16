package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class PageRankComputationBasic extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(PageRankComputationBasic.class);
  private static final String JOB_NAME_TEMPLATE = "PageRank:Wikipedia:basic:compute:%s:%s";

  private static final float ALPHA = 0.15f;

  public static void main(final String[] args) throws Exception {
    log.info("main ARGS: " + Arrays.asList(args));
    ToolRunner.run(new PageRankComputationBasic(), args);
  }

  String basepath;
  int nodes;

  @Override
  public int run(final String[] args) throws Exception {
    final CmdArgs cmdargs = CmdArgs.parse(args);
    assertTrue(cmdargs.getStartIteration() <= cmdargs.getEndIteration());
    basepath = cmdargs.getBasepath();
    nodes = cmdargs.getNodes();
    log.info(strfmt("basepath '%s', nodes '%s', start '%s', end '%s'",
        basepath, nodes, cmdargs.getStartIteration(), cmdargs.getEndIteration()));

    for (int i = cmdargs.getStartIteration(); i <= cmdargs.getEndIteration(); i++) {
      log.info("doing iteration " + i);
      if (!phase1(i)) {
        log.error("iteration " + i + " failed");
        break;
      }
    }

    return 0;
  }

  private final boolean phase1(final int iteration) throws Exception {
    final JobBuilder builder = JobBuilder.build(Job.getInstance(getConf(),
        String.format(JOB_NAME_TEMPLATE, iteration, "phase1")));

    final String inputpath = new File(basepath, iterDirName(iteration)).getCanonicalPath();
    final String outputDir = new File(basepath, iterDirName(iteration + 1)).getCanonicalPath();

    builder.inputPaths(new Path(inputpath)).outputPath(new Path(outputDir));
    builder.inputFormatClass(PageInputFormat.class).outputFormatClass(TextOutputFormat.class);
    builder.mapOutputKeyClass(Text.class).mapOutputValueClass(Page.class);
    builder.outputKeyClass(Text.class).outputValueClass(Page.class);
    builder.mapperClass(TheMapper.class).reducerClass(TheReducer.class);
    builder.setInt(NODES_KEY, nodes).setInt(ITERATION_NUM_KEY, iteration);

    final Job job = builder.job();

    deleteDirs(job.getConfiguration(), outputDir);

    final long startTime = System.nanoTime();
    final boolean status = job.waitForCompletion(true);
    log.info("job " + job.getJobID() + "completed in " + elapsed(startTime) + " seconds with status " + status);

    return status;
  }

  public static class TheMapper extends Mapper<Text, Page, Text, Page> {
    private final Page intermediateStructure = new Page();
    private final Page intermediateMass = new Page();
    private final Text neighbor = new Text();

    @Override
    protected void map(final Text title, final Page page, final Context context) throws IOException, InterruptedException {
      final int iterationNum = context.getConfiguration().getInt(ITERATION_NUM_KEY, 0);
      assertTrue(iterationNum > 0);

      if (iterationNum == 1) {
        page.rank = 1.0f; // set page rank for first iteration
      }

      assertTrue(page.type != Page.Type.INVALID);

      intermediateStructure.type = Page.Type.STRUCTURE;
      intermediateStructure.adjacencyList = page.adjacencyList;

      context.write(title, intermediateStructure); // <<<<< write structure

      final int nEdges = page.adjacencyList == null ? 0 : page.adjacencyList.size();
      int massMessages = 0;

      if (nEdges > 0) {
        final float mass = page.rank / nEdges;
        context.getCounter(Counters.EDGES).increment(nEdges);
        for (final String node : page.adjacencyList) {
          intermediateMass.type = Page.Type.MASS;
          intermediateMass.rank = mass;
          neighbor.set(node);
          context.write(neighbor, intermediateMass); // <<<<< write mass
          massMessages++;
        }
      }

      context.getCounter(Counters.NODES).increment(1);
      context.getCounter(Counters.MASS_MESSAGES_SENT).increment(massMessages);
    }
  }

  public static class TheReducer extends Reducer<Text, Page, Text, Page> {
    @Override
    protected void reduce(final Text title, final Iterable<Page> pages, final Context context) throws IOException, InterruptedException {

      final Page node = new Page();
      node.type = Page.Type.COMPLETED;

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float mass = 0.0f;

      for (final Page page : pages) {
        if (page.type == Page.Type.STRUCTURE) {
          node.adjacencyList = page.adjacencyList;
          structureReceived++;
        } else if (page.type == Page.Type.MASS) {
          mass += page.rank;
          massMessagesReceived++;
        } else {
          throw new AssertionError("invalid page type received '" + page.type + "'");
        }
      }

      node.rank = (1 - ALPHA) + ALPHA * mass;

      context.getCounter(Counters.MASS_MESSAGES_RCVD).increment(massMessagesReceived);

      if (structureReceived == 1) {
        context.write(title, node); // <==================== write reduce output
      } else if (structureReceived == 0) {
        context.getCounter(Counters.MISSING_STRUCTURE).increment(1);
        log.warn("No structure received for node: '" + title + "' mass: " + massMessagesReceived);
      } else {
        throw new AssertionError("multiple structures received for '" + title + "'");
      }
    }
  }

}
