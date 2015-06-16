package edu.newhaven.sgurjar.wikipedia;

public class CmdArgs {
  public static final CmdArgs parse(final String[] args) {
    try {
      final CmdArgs cmdargs = new CmdArgs();
      for (int i = 0; i < args.length; i++) {
        if ("-basepath".equals(args[i])) {
          cmdargs.basepath = args[++i];
        }
        else if ("-nodes".equals(args[i])) {
          cmdargs.nodes = Integer.parseInt(args[++i]);
        }
        else if ("-start".equals(args[i])) {
          cmdargs.startIteration = Integer.parseInt(args[++i]);
        }
        else if ("-end".equals(args[i])) {
          cmdargs.endIteration = Integer.parseInt(args[++i]);
        }
      }
      if (cmdargs.basepath == null || (cmdargs.basepath = cmdargs.basepath.trim()).isEmpty())
        throw new UsageException("invalid -basepath '" + cmdargs.basepath + "'");
      if (cmdargs.startIteration < 0)
        throw new UsageException("invalid -start '" + cmdargs.startIteration + "'");
      if (cmdargs.endIteration < 1)
        throw new UsageException("invalid -end '" + cmdargs.endIteration + "'");
      if (cmdargs.nodes < 1)
        throw new UsageException("invalid -nodes '" + cmdargs.nodes + "'");
      return cmdargs;
    } catch (final Throwable e) {
      if (e instanceof UsageException)
        throw (UsageException) e;
      else
        throw new UsageException(e);
    }
  }

  @SuppressWarnings("serial")
  public static class UsageException extends RuntimeException {
    public UsageException() {
      super();
    }

    public UsageException(final String msg) {
      super(msg);
    }

    public UsageException(final Throwable cause) {
      super(cause);
    }
  }

  private String basepath;
  private int nodes = -1;
  private int startIteration = -1, endIteration = -1;

  public String getBasepath() {
    return basepath;
  }

  public int getNodes() {
    return nodes;
  }

  public int getStartIteration() {
    return startIteration;
  }

  public int getEndIteration() {
    return endIteration;
  }
}
