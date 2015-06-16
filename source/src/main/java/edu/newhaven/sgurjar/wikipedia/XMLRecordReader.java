package edu.newhaven.sgurjar.wikipedia;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/*
 * https://github.com/lintool/Cloud9/blob/master/src/main/java/edu/umd/cloud9/collection/XMLInputFormat.java
 */
public class XMLRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger log = Logger.getLogger(XMLRecordReader.class);

  public static final String DEFAULT_START_TAG = "<page>";
  public static final String DEFAULT_END_TAG = "</page>";
  public static final String START_TAG_KEY = "xml.tag.start";
  public static final String END_TAG_KEY = "xml.tag.end";

  private byte[] _startXmlTag, _endXmlTag;
  private long _splitStartOffset, _splitEndPos, _splitCurrentPos, _xmlTagStartPos;
  private DataInputStream _dataInputStream;
  private final DataOutputBuffer _dataOutputBuffer = new DataOutputBuffer();
  private final LongWritable _key = new LongWritable();
  private final Text _value = new Text();

  @Override
  public void initialize(final InputSplit inputsplit, final TaskAttemptContext context)
      throws IOException, InterruptedException {
    final Configuration configuration = context.getConfiguration();

    _startXmlTag = configuration.get(START_TAG_KEY, DEFAULT_START_TAG).getBytes(Globals.UTF_8);
    _endXmlTag = configuration.get(END_TAG_KEY, DEFAULT_END_TAG).getBytes(Globals.UTF_8);

    final FileSplit split = (FileSplit) inputsplit;

    _splitStartOffset = split.getStart();

    final Path file = split.getPath();
    final FSDataInputStream in = file.getFileSystem(configuration).open(file);

    final CompressionCodec compressed = new CompressionCodecFactory(configuration).getCodec(file);
    if (compressed != null) {
      log.debug("INIT001 reading compressed file " + file);
      _dataInputStream = new DataInputStream(compressed.createInputStream(in));
      _splitEndPos = Long.MAX_VALUE; // ???? do we use non-splittable InputFormat? how about bzip2 ?
    } else {
      log.debug("INIT001 reading file " + file);
      in.seek(_splitStartOffset);
      _dataInputStream = in;
      _splitEndPos = _splitStartOffset + split.getLength();
    }

    _splitCurrentPos = _xmlTagStartPos = _splitStartOffset;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (_splitCurrentPos < _splitEndPos) {
      if (readUntilMatch(_startXmlTag, false/* not found startXmlTag yet */)) {
        _xmlTagStartPos = _splitCurrentPos - _startXmlTag.length; // include the <page> tag
        try {
          _dataOutputBuffer.write(_startXmlTag); // write <page> on the _readBuf as it was skipped in readUntilMatch
          if (readUntilMatch(_endXmlTag, true/* found _startXmlTag */)) {
            _key.set(_xmlTagStartPos);
            _value.set(_dataOutputBuffer.getData(), 0, _dataOutputBuffer.getLength());
            return true;
          }
        } finally {
          if (_dataInputStream instanceof Seekable) { // sanity check
            if (_splitCurrentPos != ((Seekable) _dataInputStream).getPos()) {
              throw new AssertionError("error check failed, consumed more bytes than counted for");
            }
          }
          _dataOutputBuffer.reset();
        }
      }
    }
    return false;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return _key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return _value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return ((float) (_splitCurrentPos - _splitStartOffset)) // this is how much we read so far
        / ((float) (_splitEndPos - _splitStartOffset)) // this is how much we need read in total
    ;
    // how about when we go past _splitEndPos and read next split if record crosses
    // the split boundary progress would be more than 100% ?
  }

  @Override
  public void close() throws IOException {
    if (_dataInputStream != null) {
      _dataInputStream.close();
    }
  }

  /* foundStartTag means we have found <page> and now we are reading until we find </page> */
  private boolean readUntilMatch(final byte[] match, final boolean foundStartTag)
      throws IOException {
    for (int i = 0, b = -1;;) {
      b = _dataInputStream.read();
      _splitCurrentPos++;
      if (b == -1) {
        return false; // end of stream
      }
      if (foundStartTag) {
        _dataOutputBuffer.write(b);
      }
      if (b == match[i]) {
        i++;
        if (i >= match.length) {
          return true; // found match
        }
      } else {
        i = 0; // no match, reset i
      }
      if (!foundStartTag && i == 0 && _splitCurrentPos >= _splitEndPos) {
        // - if we are not reading in <page></page> block
        // - and we didn't find match
        // - and we reached to end of split
        // we are done
        return false;
      }
      // as oppose to, if we are reading within the block, that is we have already found <page>
      // earlier to this call we continue read past the _splitEndOffset until we find </page>
      // if start of page '<page>' and end of page '</page>' crosses the split boundary there
      // might be _remote_ reads to adjacent split.
    }
  }
}
