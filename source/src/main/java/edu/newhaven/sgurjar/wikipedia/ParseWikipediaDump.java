package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ParseWikipediaDump extends Configured implements Tool {

  public static void main(final String[] args) throws Exception {
    ToolRunner.run(new ParseWikipediaDump(), args);
  }

  private static final Logger log = Logger.getLogger(ParseWikipediaDump.class);

  public enum ParsePageCounters {
    TOTAL_PAGES, NO_TITLE, EMPTY_TITLE, NO_TEXT, EMPTY_TEXT, REDIRECT, DISAMBIGUATION, STUB, NO_LINKS, NOT_ARTICLE
  }

  public enum GraphCounters {
    VERTICES, EDGES
  }

  @Override
  public int run(final String[] args) throws Exception {
    log.info("ARGS: " + Arrays.asList(args));

    if (args.length < 2) {
      System.out.println("error: at least 2 arguments are required, <inputfiles> <outputpath>");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    final String infiles = args[0];
    final String outdir = args[1];

    final Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Wikipedia:parse");
    job.setJarByClass(ParseWikipediaDump.class);

    FileInputFormat.setInputPaths(job, new Path(infiles));
    FileOutputFormat.setOutputPath(job, new Path(outdir));

    job.setInputFormatClass(XMLInputFormat.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapSpeculativeExecution(false);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // delete outdir
    FileSystem.get(getConf()).delete(new Path(outdir), true);

    final long startTime = System.nanoTime();
    job.waitForCompletion(true);
    log.info("Job Finished in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) + " seconds");

    final PrintStream counterfile = new PrintStream(FileSystem.get(getConf()).create(
        new Path(outdir, job.getJobID() + "-counters.txt"), false/* overwrite */));
    try {
      final Counters counters = job.getCounters();
      if (counters != null) {
        counterfile.println(counters.toString());
      }
    } finally {
      counterfile.close();
    }

    return 0;
  }

  //
  // ========= Mapper
  //
  public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    static final String TITLE_START = "<title>";
    static final String TITLE_END = "</title>";
    static final String TEXT_START_PREFIX = "<text", TEXT_START_SUFFIX = ">";
    static final String TEXT_END = "</text>";
    static final Pattern RE_REDIRECT = Pattern.compile("#REDIRECT\\s*\\[\\[(.*?)\\]\\]", Pattern.CASE_INSENSITIVE);
    static final Pattern RE_DISAMB = Pattern.compile("\\{\\{disambig\\w*\\}\\}", Pattern.CASE_INSENSITIVE);
    static final String STUB_NS = "Wikipedia:Stub";
    static final String STUB_TEMPLATE = "-stub}}";
    static final String NAMESPACE_START = "<ns>";
    static final String NAMESPACE_END = "</ns>";

    private int start = -1, end = -1, pos = -1, linkCount = 0;
    private String title, text;
    private final Text outputKey = new Text(), outputValue = new Text();
    private boolean isArticle = false;

    @Override
    protected void map(final LongWritable key, final Text page, final Context context) throws IOException, InterruptedException {

      context.getCounter(ParsePageCounters.TOTAL_PAGES).increment(1);

      // isArticle ? <ns>0</ns>
      isArticle = false;
      start = page.find(NAMESPACE_START);
      if (start != -1) {
        start += NAMESPACE_START.length();
        end = page.find(NAMESPACE_END, start);
        if (end != -1) {
          isArticle = cut(page, start, end).trim().equals("0");
        }
      } else {
        isArticle = true; // if tag not found then its article
      }

      if (!isArticle) {
        context.getCounter(ParsePageCounters.NOT_ARTICLE).increment(1);
        return;
      }

      // <title>
      start = page.find(TITLE_START);
      if (start == -1) {
        log.warn("TITLE_START_NOT_FOUND key '" + key + "', start '" + start + "'");
        context.getCounter(ParsePageCounters.NO_TITLE).increment(1);
        return;
      }
      start += TITLE_START.length();
      end = page.find(TITLE_END, start);
      if (end == -1) {
        context.getCounter(ParsePageCounters.NO_TITLE).increment(1);
        log.warn("TITLE_END_NOT_FOUND key '" + key + "', start '" + start + "' end '" + end + "'");
        return;
      }
      title = cut(page, start, end);
      if (title.isEmpty()) {
        context.getCounter(ParsePageCounters.EMPTY_TITLE).increment(1);
        log.warn("EMPTY_TITLE_FOUND key '" + key + "', start '" + start + "' end '" + end + "' title '" + title + "'");
        return;
      }
      title = StringEscapeUtils.unescapeHtml(title).replaceAll(" ", "_");

      // text
      start = page.find(TEXT_START_PREFIX);
      if (start == -1) {
        context.getCounter(ParsePageCounters.NO_TEXT).increment(1);
        log.warn("TEXT_START_PREFIX_NOT_FOUND key '" + key + "', start '" + start + "' title '" + title + "'");
        return;
      }
      start += TEXT_START_PREFIX.length();
      start = page.find(TEXT_START_SUFFIX, start);
      if (start == -1) {
        context.getCounter(ParsePageCounters.NO_TEXT).increment(1);
        log.warn("TEXT_START_SUFFIX_NOT_FOUND key '" + key + "', start '" + start + "' title '" + title + "'");
        return;
      }
      start += TEXT_START_SUFFIX.length();
      end = page.find(TEXT_END, start);
      if (end == -1) {
        context.getCounter(ParsePageCounters.NO_TEXT).increment(1);
        log.warn("TEXT_END_NOT_FOUND key '" + key + "', start '" + start + "' end '" + end + "' title '" + title + "'");
        return;
      }
      text = cut(page, start, end);
      if (text.isEmpty()) {
        context.getCounter(ParsePageCounters.EMPTY_TEXT).increment(1);
        log.warn("EMPTY_TEXT_FOUND key '" + key + "', start '" + start + "' end '" + end + "' text '" + text + "' title '" + title + "'");
        return;
      }

      // redirect
      // some redirects only have <redirect tag
      // <redirect title="Troll (Internet)"/>
      if (RE_REDIRECT.matcher(text).find()) {
        context.getCounter(ParsePageCounters.REDIRECT).increment(1);
        return;
      }

      // disambiguation
      if (RE_DISAMB.matcher(text).find()) {
        context.getCounter(ParsePageCounters.DISAMBIGUATION).increment(1);
        return;
      }

      // stub
      if (page.find(STUB_NS) != -1 || text.indexOf(STUB_TEMPLATE) != -1) {
        context.getCounter(ParsePageCounters.STUB).increment(1);
        return;
      }

      // links
      outputKey.set(title);

      linkCount = start = 0;
      pos = end = -1;

      while (true) {
        start = text.indexOf("[[", start);
        if (start < 0) {
          break;
        }

        start = start + 2;
        end = text.indexOf("]]", start);
        if (end < 0) {
          break;
        }

        String link = text.substring(start, end);

        if (!link.isEmpty()) { // skip empty links
          if (link.indexOf(":") == -1) { // skip special links
            if ((pos = link.indexOf("|")) != -1) {
              link = link.substring(0, pos);
            }
            if ((pos = link.indexOf("#")) != -1) {
              link = link.substring(0, pos);
            }
            if (link.length() > 0) { // ignore article-internal links, e.g., [[#section|here]]
              outputValue.set(StringEscapeUtils.unescapeHtml(link).replaceAll(" ", "_"));
              context.write(outputKey, outputValue);
              linkCount++;
            }
          }
        }
        start = end + 1;
      }

      if (linkCount == 0) {
        context.getCounter(ParsePageCounters.NO_LINKS).increment(1);
      }
    }
  } // end of MyMapper class

  //
  // ========= Reducer
  //
  public static class MyReducer extends Reducer<Text, Text, Text, Page> {

    private final Page page = new Page();
    private int nEdges;

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
      context.getCounter(GraphCounters.VERTICES).increment(1);

      // TITLE PAGE_TYPE PAGERANK LINKS
      // title 0 - link1 link2 link3 ....
      page.clear();
      page.type = Page.Type.INITIALIZED;

      nEdges = 0;

      for (final Text value : values) {
        page.adjacencyList.add(value.toString());
        nEdges++;
      }

      context.getCounter(GraphCounters.EDGES).increment(nEdges);
      context.write(key, page);
    }
  }
}
