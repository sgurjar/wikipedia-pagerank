package edu.newhaven.sgurjar.wikipedia;

import java.net.URI;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class PageSeqFileReader {

  public static void main(final String[] args) throws Exception {
    final String uri = args[0];
    final int nLines = (args.length > 1) ? Integer.parseInt(args[1]) : 0;
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    SequenceFile.Reader reader = null;
    try {
      path = path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
      System.out.printf("### path '%s' nLines %d \n", path, nLines);
      reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

      final Class<?> keyClass = reader.getKeyClass();
      final Class<?> valueClass = reader.getValueClass();

      System.out.printf("### KeyClass=%s,ValueClass=%s\n", keyClass, valueClass);
      // ### KeyClass=class org.apache.hadoop.io.Text,ValueClass=class edu.newhaven.sgurjar.wikipedia.Page

      final Writable key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
      final Writable value = (Writable) ReflectionUtils.newInstance(valueClass, conf);

      long nodeCount = 0, edgeCount = 0;
      final EnumMap<Page.Type, Long> countByPageTypes = new EnumMap<Page.Type, Long>(Page.Type.class);
      Long typeCount = 0L;
      //long position = reader.getPosition();
      while (reader.next(key, value)) {
        //final String syncSeen = reader.syncSeen() ? "*" : "";
        final Page page = (Page) value;
        typeCount = countByPageTypes.get(page.type);
        if (typeCount == null)
          countByPageTypes.put(page.type, 1L);
        else
          countByPageTypes.put(page.type, typeCount + 1);
        nodeCount++;
        edgeCount += page.adjacencyList == null ? 0 : page.adjacencyList.size();
        System.out.println(key + "\t" + page.rank + "\t" + toString(page.adjacencyList, 5));
        //System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        //position = reader.getPosition(); // beginning of next record
        if (nLines > 0 && nodeCount >= nLines) break;
      }
      System.out.println("### Nodes: " + nodeCount);
      System.out.println("### Edges: " + edgeCount);
      for (final Map.Entry<Page.Type, Long> e : countByPageTypes.entrySet()) {
        System.out.println("### PageType " + e.getKey() + ": " + e.getValue());
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  static String toString(final Set<String> aset, int count) {
    final StringBuilder sb = new StringBuilder();
    final Iterator<String> it = aset.iterator();
    if (it.hasNext()) {
      sb.append('<').append(it.next());
      count--;
    }
    while (count-- > 0 && it.hasNext()) {
      sb.append('\t').append(it.next());
    }
    if (it.hasNext()) sb.append(" ...");
    return sb.append('>').toString();
  }
}
