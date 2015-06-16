package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.log4j.Logger;

/* takes Text as input returns Page object */
public class PageInputFormat extends FileInputFormat<Text, Page> {

  @Override
  public RecordReader<Text, Page> createRecordReader(final InputSplit split,
      final TaskAttemptContext context) throws IOException, InterruptedException {
    return new PageRecordReader(context.getConfiguration());
  }

  public static class PageRecordReader extends RecordReader<Text, Page> {
    private static final Logger log = Logger.getLogger(PageRecordReader.class);

    private final KeyValueLineRecordReader keyValueLineRecordReader;
    private Page page;

    public PageRecordReader(final Configuration conf) throws IOException {
      if (log.isDebugEnabled()) log.debug("ctor: " + conf);
      keyValueLineRecordReader = new KeyValueLineRecordReader(conf);
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context)
        throws IOException, InterruptedException {
      if (log.isDebugEnabled()) log.debug("initialize " + split + "," + context);
      keyValueLineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      final boolean flag = keyValueLineRecordReader.nextKeyValue();
      if (log.isDebugEnabled()) log.debug("nextKeyValue " + flag);
      return flag;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      final Text k = keyValueLineRecordReader.getCurrentKey();
      if (log.isDebugEnabled()) log.debug("getCurrentKey '" + k + "'");
      return k;
    }

    @Override
    public Page getCurrentValue() throws IOException, InterruptedException {
      final Text v = keyValueLineRecordReader.getCurrentValue();
      if (page == null) page = new Page();
      final Page p = Page.fromString(v.toString(), page);
      if (log.isDebugEnabled()) log.debug("getCurrentValue " + ref(p) + " '" + p + "'");
      return p;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return keyValueLineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      keyValueLineRecordReader.close();
    }
  }
}
