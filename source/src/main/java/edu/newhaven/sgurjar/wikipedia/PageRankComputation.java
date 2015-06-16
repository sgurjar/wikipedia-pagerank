package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/*
 * PageRank P of a page n is defined as follows
 *
 * P(n) = (ALPHA * (1/|G|)) + ((1 - ALPHA) * SUM(P(m)/C(m))) where m in L(n)
 *
 * |G| total number of nodes in the Graph ALPHA random jump factor, also called "teleportation"
 * factor L(n) inbound links to page n C(m) out-degree of node m
 */

public class PageRankComputation extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(PageRankComputation.class);

  private static final String JOB_NAME_TEMPLATE = "PageRank:Wikipedia:logarithmic:compute:%s:%s";
  private static final float ALPHA = 0.15f;

  public static void main(final String[] args) throws Exception {
    log.info("main ARGS: " + Arrays.asList(args));
    ToolRunner.run(new PageRankComputation(), args);
  }

  private String basepath;
  private int nodes;

  @Override
  public int run(final String[] args) throws Exception {
    final CmdArgs cmdargs = CmdArgs.parse(args);

    assertTrue(cmdargs.getStartIteration() <= cmdargs.getEndIteration());

    basepath = cmdargs.getBasepath();
    nodes = cmdargs.getNodes();

    log.info(strfmt("basepath '%s', nodes '%s', start '%s', end '%s'",
        basepath, nodes, cmdargs.getStartIteration(), cmdargs.getEndIteration()));

    boolean status = false;
    for (int i = cmdargs.getStartIteration(); i <= cmdargs.getEndIteration(); i++) {
      log.info("doing iteration " + i);

      final float totalMass = phase1(i);

      log.info("totalMass " + totalMass);

      // mass is probability distribution
      final float missingMass = 1.0f - (float) StrictMath.exp(totalMass);

      log.info("missingMass " + missingMass);

      status = phase2(i, missingMass);

      log.info("phase2 returned " + status);

      if (!status) break;
    }

    return status ? 0 : -1;
  }

  // iter001 -> iter001t
  private final float phase1(final int iteration) throws Exception {

    final Job job = Job.getInstance(getConf(), String.format(JOB_NAME_TEMPLATE, iteration, "phase1"));
    final JobBuilder builder = JobBuilder.build(job).setJarByClass(PageRankComputation.class);

    final String dirName = iterDirName(iteration);
    final String massOutputDir = new File(basepath, dirName + "-mass").getCanonicalPath();
    final String inputpath = new File(basepath, dirName).getCanonicalPath();
    final String outputDir = new File(basepath, dirName + "t").getCanonicalPath();

    log.info(strfmt("phase1 iteration(%s) inputpath(%s) massOutputDir(%s) outputDir(%s) nodes(%s)",
        iteration, inputpath, massOutputDir, outputDir, nodes));

    builder.setInt(NODES_KEY, nodes).setInt(ITERATION_NUM_KEY, iteration).set(MASS_OUTPUT_PATH_KEY, massOutputDir);

    builder.inputPaths(new Path(inputpath)).outputPath(new Path(outputDir));

    //builder.inputFormatClass(PageInputFormat.class).outputFormatClass(TextOutputFormat.class);
    builder.inputFormatClass(SequenceFileInputFormat.class).outputFormatClass(SequenceFileOutputFormat.class);

    builder.mapOutputKeyClass(Text.class).mapOutputValueClass(Page.class);
    builder.outputKeyClass(Text.class).outputValueClass(Page.class);
    builder.compressMapperOutput().compressReducerOutput(JobBuilder.SEQUENCE_FILE);

    builder.mapperClass(TheMapper.class).reducerClass(TheReducer.class).speculativeExecution(false);

    deleteDirs(job.getConfiguration(), massOutputDir, outputDir);

    final boolean status = runJob(job);

    if (!status) throw new AssertionError("phase1 failed");

    final float missingMass = collectMissingMass(job, massOutputDir);

    return missingMass;
  }

  // iter001t -> iter002
  private final boolean phase2(final int iteration, final float missingMass) throws Exception {

    final Job job = Job.getInstance(getConf(), String.format(JOB_NAME_TEMPLATE, iteration, "phase2"));
    final JobBuilder builder = JobBuilder.build(job).setJarByClass(PageRankComputation.class);

    final String inputpath = new File(basepath, iterDirName(iteration) + "t").getCanonicalPath();
    final String outputDir = new File(basepath, iterDirName(iteration + 1)).getCanonicalPath();

    log.info(strfmt("phase2 iteration(%s) inputpath(%s) outputDir(%s) nodes(%s) missingMass(%s)",
        iteration, inputpath, outputDir, nodes, missingMass));

    builder.setInt(NODES_KEY, nodes).setFloat(MISSING_MASS_KEY, missingMass);

    builder.inputPaths(new Path(inputpath)).outputPath(new Path(outputDir));
    //builder.inputFormatClass(PageInputFormat.class).outputFormatClass(TextOutputFormat.class);
    builder.inputFormatClass(SequenceFileInputFormat.class).outputFormatClass(SequenceFileOutputFormat.class);

    builder.mapOutputKeyClass(Text.class).mapOutputValueClass(Page.class);
    builder.outputKeyClass(Text.class).outputValueClass(Page.class);
    builder.compressMapperOutput().compressReducerOutput(JobBuilder.SEQUENCE_FILE);

    builder.mapperClass(MassDistributionMapper.class).numReduceTasks(0).speculativeExecution(false);

    deleteDirs(job.getConfiguration(), outputDir);

    return runJob(job);
  }

  private static float collectMissingMass(final Job job, final String massOutputDir) throws IOException {
    float missingmass = Float.NEGATIVE_INFINITY;
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    for (final FileStatus f : fs.listStatus(new Path(massOutputDir))) {
      final FSDataInputStream fin = fs.open(f.getPath());
      missingmass = sumOfLogProbs(missingmass, fin.readFloat());
      fin.close();
    }
    return missingmass;
  }

  private static boolean runJob(final Job job) throws Exception {
    final long startTime = System.nanoTime();
    final boolean ok = job.waitForCompletion(true);
    log.info("job " + job.getJobID() + "completed in " + elapsed(startTime) + " seconds with status " + ok);
    return ok;
  }

  public static class TheMapper extends Mapper<Text, Page, Text, Page> {
    private final Page intermediateStructure = new Page();
    private final Page intermediateMass = new Page();
    private final Text neighbor = new Text();

    @Override
    protected void map(final Text title, final Page page, final Context context)
        throws IOException, InterruptedException {

      final int iterationNum = context.getConfiguration().getInt(ITERATION_NUM_KEY, 0);
      assertTrue(iterationNum > 0);

      if (iterationNum == 1) {
        // set page rank for first iteration
        final int nodes = context.getConfiguration().getInt(NODES_KEY, 0);
        assertTrue(nodes > 0);
        page.rank = (float) -StrictMath.log(nodes);
      }

      assertTrue(page.type != Page.Type.INVALID);
      // assertTrue(page.adjacencyList != null); can be null if a node has no outgoing link

      intermediateStructure.type = Page.Type.STRUCTURE;
      intermediateStructure.adjacencyList = page.adjacencyList;
      // rank of intermediateStructure doesn't matter ?
      // ** intermediateStructure will always have rank of Float.NEGATIVE_INFINITY

      context.write(title, intermediateStructure);

      final int nEdges = page.adjacencyList == null ? 0 : page.adjacencyList.size();
      int massMessages = 0;

      if (nEdges > 0) {
        final float mass = page.rank - (float) StrictMath.log(nEdges);
        context.getCounter(Counters.EDGES).increment(nEdges);
        for (final String node : page.adjacencyList) {
          intermediateMass.type = Page.Type.MASS;
          intermediateMass.rank = mass;
          neighbor.set(node);
          context.write(neighbor, intermediateMass);
          // adjacencyList of mass nodes doesn't matter
          // ** adjacencyList of mass node will always be null
          massMessages++;
        }
      }

      context.getCounter(Counters.NODES).increment(1);
      context.getCounter(Counters.MASS_MESSAGES_SENT).increment(massMessages);
    }
  }

  public static class TheReducer extends Reducer<Text, Page, Text, Page> {
    private float totalMass = Float.NEGATIVE_INFINITY;

    @Override
    protected void reduce(final Text title, final Iterable<Page> pages, final Context context)
        throws IOException, InterruptedException {

      final Page node = new Page();

      node.type = Page.Type.COMPLETED;

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float mass = Float.NEGATIVE_INFINITY;

      for (final Page page : pages) {
        if (page.type == Page.Type.STRUCTURE) {
          node.adjacencyList = page.adjacencyList;
          structureReceived++;
        } else if (page.type == Page.Type.MASS) {
          mass = sumOfLogProbs(mass, page.rank);
          massMessagesReceived++;
        } else {
          throw new AssertionError("invalid page type received '" + page.type + "'");
        }
      }

      node.rank = mass;

      context.getCounter(Counters.MASS_MESSAGES_RCVD).increment(massMessagesReceived);

      if (structureReceived == 1) {
        context.write(title, node); // <==================== write reduce output
        totalMass = sumOfLogProbs(totalMass, mass);
      } else if (structureReceived == 0) {
        context.getCounter(Counters.MISSING_STRUCTURE).increment(1);
        log.warn("No structure received for node: '" + title + "' mass: " + massMessagesReceived);
      } else {
        throw new AssertionError("multiple structures received for '" + title + "'");
      }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
      final String taskId = context.getTaskAttemptID().getTaskID().toString();
      final String massOutputPath = context.getConfiguration().get(MASS_OUTPUT_PATH_KEY);
      assertNotNullOrEmpty(massOutputPath);
      final FileSystem fs = FileSystem.get(context.getConfiguration());
      final FSDataOutputStream out =
          fs.create(new Path(massOutputPath, taskId), false/* overwrite */);
      out.writeFloat(totalMass);
      out.close();
    }
  }

  /* distributes missing pagerank mass */
  public static class MassDistributionMapper extends Mapper<Text, Page, Text, Page> {
    private float missingMass = 0.0f;
    private int nodes = 0;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      missingMass = context.getConfiguration().getFloat(MISSING_MASS_KEY, 0.0f);
      nodes = context.getConfiguration().getInt(NODES_KEY, 0);
    }

    @Override
    protected void map(final Text title, final Page page, final Context context)
        throws IOException, InterruptedException {
      final float jump = (float) (Math.log(ALPHA) - Math.log(nodes));
      final float link =
          (float) Math.log(1.0f - ALPHA)
              + sumOfLogProbs(page.rank, (float) (Math.log(missingMass) - Math.log(nodes)));

      page.rank = sumOfLogProbs(jump, link);

      context.write(title, page);
    }
  }
}
