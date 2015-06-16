package edu.newhaven.sgurjar.wikipedia;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class Globals {

  private static final Logger log = Logger.getLogger(Globals.class);

  public static final Charset UTF_8 = Charset.forName("UTF-8");
  public static final String NODES_KEY = "graph.nodes";
  public static final String MASS_OUTPUT_PATH_KEY = "mass.output.path";
  public static final String MISSING_MASS_KEY = "missing.mass";
  public static final String ITERATION_NUM_KEY = "iteration.num";

  public static String cut(final Text text, final int start, final int end) throws CharacterCodingException {
    return Text.decode(text.getBytes(), start, end - start);
  }

  public static String assertNotNullOrEmpty(final String s) {
    return assertNotNullOrEmpty(s, "string is null or empty");
  }

  public static String assertNotNullOrEmpty(final String s, final String failedMsg) {
    if (s == null || s.isEmpty()) {
      throw new AssertionError(failedMsg + ": '" + s + "'");
    }
    return s;
  }

  public static <T> T assertNotNull(final T ref) {
    if (ref == null) {
      throw new AssertionError();
    }
    return ref;
  }

  public static void assertTrue(final boolean condition, final String msg) {
    if (!condition) {
      throw new AssertionError(msg);
    }
  }

  public static void assertTrue(final boolean condition) {
    if (!condition) {
      throw new AssertionError();
    }
  }

  public static String stackTrace(final Throwable t) {
    final StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  public static String ref(final Object obj) {
    if (obj == null)
      return "null";
    else
      return obj.getClass() + "@" + System.identityHashCode(obj);
  }

  public static long elapsed(final long startNano) {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNano);
  }

  public static void deleteDirs(final Configuration conf, final String... dirs) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    for (final String dir : dirs) {
      final Path path = new Path(dir);
      log.info("deleting '" + path + "' from " + fs.getUri());
      fs.delete(path, true/*recursive*/);
    }
  }

  /*
   * Data-intensive text processing with MapReduce -Lin, Jimmy, and Chris Dyer
   * 5.4 Issues with Graph Processing
   *
   * Finally, there is a practical consideration to keep in mind when implementing
   * graph algorithms that estimate probability distributions over nodes (such as PageRank).
   * For large graphs, the probability of any particular node is often so small that it under
   * flows standard floating point representations. A very common solution to this problem is
   * to represent probabilities using their logarithms. When probabilities are stored as logs,
   * the product of two values is simply their sum. However, addition of probabilities
   * is also necessary, for example, when summing PageRank contribution for a node.
   * This can be implemented with reasonable precision as follows:
   *
   *                b + log(1 + e^(a-b))      a < b
   *      a + b =
   *                a + log(1 + e^(b-a))      b <= a
   *
   * Furthermore, many math libraries include a log1p function which computes
   * log(1+x) with higher precision than the native implementation would have when
   * x is very small (as is often the case when working with probabilities). Its use
   * may further improve the accuracy of implementations that use log probabilities.
   *
   */
  public static float sumOfLogProbs(final float a, final float b) {
    if (a == Float.NEGATIVE_INFINITY) return b;
    if (b == Float.NEGATIVE_INFINITY) return a;

    if (a < b)
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    /*b >= a */else
      return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }

  public static String strfmt(String template, final Object... args) {
    template = String.valueOf(template); // null -> "null"

    // start substituting the arguments into the '%s' placeholders
    final StringBuilder builder = new StringBuilder(
        template.length() + 16 * args.length);
    int templateStart = 0;
    int i = 0;
    while (i < args.length) {
      final int placeholderStart = template.indexOf("%s", templateStart);
      if (placeholderStart == -1) {
        break;
      }
      builder.append(template.substring(templateStart, placeholderStart));
      builder.append(args[i++]);
      templateStart = placeholderStart + 2;
    }
    builder.append(template.substring(templateStart));

    // if we run out of placeholders, append the extra args in square braces
    if (i < args.length) {
      builder.append(" [");
      builder.append(args[i++]);
      while (i < args.length) {
        builder.append(", ");
        builder.append(args[i++]);
      }
      builder.append(']');
    }

    return builder.toString();
  }

  public static String iterDirName(final int i) {
    return String.format("iter%02d", i);
  }

}
