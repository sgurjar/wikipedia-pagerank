package edu.newhaven.sgurjar.wikipedia;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XMLInputFormat extends TextInputFormat {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
      final InputSplit split,
      final TaskAttemptContext context) {
    return new XMLRecordReader();
  }

  @Override
  protected boolean isSplitable(final JobContext context, final Path file) {
    return false; // <<<<<<<<
  }
}
