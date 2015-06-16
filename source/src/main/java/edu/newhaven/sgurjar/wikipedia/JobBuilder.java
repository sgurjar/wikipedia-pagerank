package edu.newhaven.sgurjar.wikipedia;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class JobBuilder {

  public static boolean SEQUENCE_FILE = true;

  public static JobBuilder build(final Job job) {
    return new JobBuilder(job);
  }

  private final Job job;

  private JobBuilder(final Job job) {
    this.job = job;
  }

  public Job job() {
    return job;
  }

  public JobBuilder setInt(final String name, final int value) {
    job.getConfiguration().setInt(name, value);
    return this;
  }

  public JobBuilder set(final String name, final String value) {
    job.getConfiguration().set(name, value);
    return this;
  }

  public JobBuilder setFloat(final String name, final float value) {
    job.getConfiguration().setFloat(name, value);
    return this;
  }

  public JobBuilder setJarByClass(final Class<?> cls) {
    job.setJarByClass(cls);
    return this;
  }

  public JobBuilder inputPaths(final Path... inputPaths) throws IOException {
    FileInputFormat.setInputPaths(job, inputPaths);
    return this;
  }

  public JobBuilder outputPath(final Path outputDir) {
    FileOutputFormat.setOutputPath(job, outputDir);
    return this;
  }

  public JobBuilder inputFormatClass(final Class<? extends InputFormat> cls) {
    job.setInputFormatClass(cls);
    return this;
  }

  public JobBuilder outputFormatClass(final Class<? extends OutputFormat> cls) {
    job.setOutputFormatClass(cls);
    return this;
  }

  public JobBuilder mapOutputKeyClass(final Class<?> theClass) {
    job.setMapOutputKeyClass(theClass);
    return this;
  }

  public JobBuilder mapOutputValueClass(final Class<?> theClass) {
    job.setMapOutputValueClass(theClass);
    return this;
  }

  public JobBuilder outputKeyClass(final Class<?> theClass) {
    job.setOutputKeyClass(theClass);
    return this;
  }

  public JobBuilder outputValueClass(final Class<?> theClass) {
    job.setOutputValueClass(theClass);
    return this;
  }

  public JobBuilder mapperClass(final Class<? extends Mapper> cls) {
    job.setMapperClass(cls);
    return this;
  }

  public JobBuilder reducerClass(final Class<? extends Reducer> cls) {
    job.setReducerClass(cls);
    return this;
  }

  public JobBuilder numReduceTasks(final int tasks) {
    job.setNumReduceTasks(tasks);
    return this;
  }

  public JobBuilder speculativeExecution(final boolean speculativeExecution) {
    job.setSpeculativeExecution(speculativeExecution);
    return this;
  }

  public JobBuilder compressMapperOutput() {
    job.getConfiguration().setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    job.getConfiguration().setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class, CompressionCodec.class);
    return this;
  }

  public JobBuilder compressReducerOutput(final boolean sequenceFile) {
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

    if (sequenceFile) {
      // this is used by SequenceFile.Writer only
      SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    }

    return this;
  }
}
