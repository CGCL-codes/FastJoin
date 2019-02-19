package soj.biclique;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;

//import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;

//import static org.apache.storm.shade.org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;
import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newLinkedList;

import soj.util.FileWriter;
import static soj.util.SchemaUtils.getFieldIdx;
import static soj.util.TimeUtils.getTimestamp;

public class TopologyArgs implements Serializable
{
    private static final Logger LOG = getLogger(TopologyArgs.class);

    private static final int SCREEN_WIDTH = 80;

    private final String _mainclassName;

    public TopologyArgs(String className) {
        _mainclassName = className;
    }

    private static final String R_SCHEMA = "r1, r2, r3";
    private static final String S_SCHEMA = "s1, s2, s3, s4";
    private static final String R_JOIN_FIELD = "r1";
    private static final String S_JOIN_FIELD = "s1";
    private static final String JOIN_OPERATOR = "=";

    private static final String TOPOLOGY_NAME = "bistream";

    private static final int DEFAULT_INT_LOWER = 0;
    private static final int DEFAULT_INT_UPPER = 20000;
    private static final int DEFAULT_DOUBLE_LOWER = 0;
    private static final int DEFAULT_DOUBLE_UPPER = 20000;
    private static final int DEFAULT_CHARS_LENGTH = 20;

    public static final String HASH_STRATEGY = "hash";
    public static final String RANDOM_STRATEGY = "random";
    public static final String FAST_JOIN_STRATEGY = "fastjoin";
    public static final String CONTRAND_STRATEGY = "contrand";
    @Option(name = "-R", aliases = "--schema-r", metaVar = "<str>",
            usage = "schema of relation R [def:\"" + R_SCHEMA + "\"]")
    public String schemaR = R_SCHEMA;

    @Option(name = "-S", aliases = "--schema-s", metaVar = "<str>",
            usage = "schema of relation R [def:\"" + S_SCHEMA + "\"]")
    public String schemaS = S_SCHEMA;

    @Option(name = "-jr", aliases = "--join-r", metaVar = "<str>",
            depends = "-R", usage = "join field of relation R [def:\""
                    + R_JOIN_FIELD + "\"]" + "\nDEPENDENCY: -R")
    public String joinFieldR = R_JOIN_FIELD;

    @Option(name = "-js", aliases = "--join-s", metaVar = "<str>",
            depends = "-S", usage = "join field of relation S [def:\""
                    + S_JOIN_FIELD + "\"]" + "\nDEPENDENCY: -S")
    public String joinFieldS = S_JOIN_FIELD;

    @Option(name = "-op", aliases = "--operator", metaVar = "<str>",
            usage = "join operator [def:\"" + JOIN_OPERATOR + "\"]")
    public String operator = JOIN_OPERATOR;

    @Option(name = "--name", metaVar = "<str>", usage = "topology name [def:"
            + TOPOLOGY_NAME + "]")
    public String topologyName = TOPOLOGY_NAME;

    @Option(name = "--remote",
            usage = "run topology in cluster (remote mode) [def:true]")
    public boolean remoteMode = true;

    @Option(name = "-lrt", aliases = "--local-runtime", metaVar = "<num>",
            usage = "running time (in seconds) for local mode [def:5]")
    public int localRuntime = 5;

    @Option(name = "--debug", usage = "enable debug logs [def:false]")
    public boolean debug = false;

    @Option(name = "-n", aliases = "--worker", metaVar = "<num>",
            usage = "# workers [def:2]")
    public int numWorkers = 2;

//    @Option(name = "-rs", aliases = "--source-rs", metaVar = "<num>",
//            usage = "# random data sources (mixing relations) [def:1]")
//    public int numGenerators = 1;
//
//    @Option(name = "-r", aliases = "--source-r", metaVar = "<num>",
//            usage = "# random data sources of relation R [def:0]")
//    public int numGeneratorsR = 0;
//
//    @Option(name = "-s", aliases = "--source-s", metaVar = "<num>",
//            usage = "# random data sources of relation S [def:0]")
//    public int numGeneratorsS = 0;
    @Option(name = "-ks", aliases = "--source-rs", metaVar = "<num>",
            usage = "# random data sources (mixing relations) [def:1]")
    public int numKafkaSpouts = 2;

    @Option(name = "-f1", aliases = "--shuffler", metaVar = "<num>",
            usage = "# shufflers [def:2]")
    public int numShufflers = 4;

    @Option(name = "-f2", aliases = "--reshuffler", metaVar = "<num>",
            usage = "# reshufflers [def:4]")
    public int numReshufflers = 4;

    @Option(name = "-pr", aliases = "--part-r", metaVar = "<num>",
            usage = "# partitions of relation R [def:4]")
    public int numPartitionsR = 8;

    @Option(name = "-ps", aliases = "--part-s", metaVar = "<num>",
            usage = "# partitions of relation S [def:4]")
    public int numPartitionsS = 8;

    @Option(name = "-g", aliases = "--gatherer", metaVar = "<num>",
            usage = "# result gatherers [def:1]")
    public int numGatherers = 1;

    @Option(name = "-trs", aliases = "--tps-rs", metaVar = "<num>",
            usage = "tuples per second generated by each random data source"
                    + " (mixing relations) [def:20]")
    public Long tuplesPerSecond = 20L;

    @Option(name = "-tr", aliases = "--tps-r", metaVar = "<num>",
            usage = "tuples per second generated by each random data source"
                    + " of relation R [def:10]")
    public Long tuplesPerSecondR = 10L;

    @Option(name = "-ts", aliases = "--tps-s", metaVar = "<num>",
            usage = "tuples per second generated by each random data source"
                    + " of relation S [def:10]")
    public Long tuplesPerSecondS = 10L;

    @Option(name = "-fluc", aliases = "--fluctuate",
            usage = "enable fluctuation for random data sources [def:false]")
    public Boolean fluctuation = false;

    @Option(name = "--index-size", metaVar = "<num>", hidden = false,
            usage = "size (in records) of each sub-index [def:2048]")
    public Integer subindexSize = 2048;

    @Option(name = "-win", aliases = "--window",
            usage = "enable sliding window [def:false]")
    public Boolean window = false;

    @Option(name = "-wr", aliases = "--window-r", metaVar = "<num>",
            usage = "window size (in seconds) for stream R [def:2]")
    public double winInSecR = 2;

    @Option(name = "-ws", aliases = "--window-s", metaVar = "<num>",
            usage = "window size (in seconds) for stream S [def:2]")
    public double winInSecS = 2;

    @Option(name = "--no-dedup", hidden = true,
            usage = "disable deduplication in the post-processing "
                    + "stage [def:false]")
    public Boolean noDedup = false;

    @Option(name = "--dedup-size", metaVar = "<num>", hidden = true,
            usage = "size (in number of records) of hash set for "
                    + "deduplication in the post-processing stage [def:500000]")
    public int dedupSize = 500000;

    @Option(name = "-agg", aliases = "--aggregate",
            usage = "enable aggregation [def:false]")
    public Boolean aggregate = false;

    @Option(name = "-ai", aliases = "--aggregate-interval", metaVar = "<num>",
            usage = "aggregation report interval (in seconds) [def:1]")
    public Long aggReportInSeconds = 1L;

    @Option(name = "--no-output",
            usage = "disable output of join results [def:false]")
    public Boolean noOutput = false;

    @Option(name = "-d", aliases = "--output-dir", metaVar = "<path>",
            usage = "output directory [def:null]")
    public String outputDir = null;


    @Option(name = "-x", aliases = "--topic")
    public String topic = "didi";
    @Option(name = "-z", aliases = "--groupid")
    public String groupid = "fastjoin";


    @Option(name = "--simple-output",
            usage = "enable simple output format for join results [def:false]")
    public Boolean simple = false;

    @Option(name = "-il", aliases = "--int-lower", metaVar = "<num>",
            usage = "lower bound of random integer [def:" + DEFAULT_INT_LOWER
                    + "]")
    public Integer intLower = DEFAULT_INT_LOWER;

    @Option(name = "-iu", aliases = "--int-upper", metaVar = "<num>",
            usage = "upper bound of random integer [def:" + DEFAULT_INT_UPPER
                    + "]")
    public Integer intUpper = DEFAULT_INT_UPPER;

    @Option(name = "-dl", aliases = "--double-lower", metaVar = "<num>",
            usage = "lower bound of random double [def:" + DEFAULT_DOUBLE_LOWER
                    + "]")
    public Integer doubleLower = DEFAULT_DOUBLE_LOWER;

    @Option(name = "-du", aliases = "--double-upper", metaVar = "<num>",
            usage = "upper bound of random double [def:" + DEFAULT_DOUBLE_UPPER
                    + "]")
    public Integer doubleUpper = DEFAULT_DOUBLE_UPPER;

    @Option(name = "-c", aliases = "--chars-length", metaVar = "<num>",
            usage = "length of random chars [def:" + DEFAULT_CHARS_LENGTH + "]")
    public Integer charsLength = DEFAULT_CHARS_LENGTH;

    @Option(name = "-h", aliases = { "-?", "--help" }, hidden = false,
            help = true, usage = "print this help message")
    public boolean help = false;

    @Option(name = "--s", metaVar = "<str>", usage = "strategy [def:"
            + HASH_STRATEGY + "]")
    public String strategy = HASH_STRATEGY;

    @Option(name = "--size", metaVar = "<str>", usage = "size [def:"
            + "10G" + "]")
    public String dataSize = "10G";

    @Option(name = "-st", aliases = "--startTime", metaVar = "<num>",
            usage = "start time of migration [def:" + "5000"
                    + "]")
    public Integer startTime = 5000;


    @Option(name = "-t", aliases = "--threshold", metaVar = "<num>",
            usage = "threshold of load imbalance[def:" + "5000"
                    + "]")
    public double threshold = 2.5;

    @Option(name = "-i", aliases = "--interval", metaVar = "<num>",
            usage = "interval of load imbalance detection[def:" + "1000"
                    + "]")
    public double interval = 1000;

    @Option(name = "-rm", aliases = "--rm", metaVar = "<num>",
            usage = "members in each group which stores r tuple[def:" + "2"
                    + "]")
    public int rMembers = 2;

    @Option(name = "-sm", aliases = "--sm", metaVar = "<num>",
            usage = "members in each group which stores s tuple[def:" + "2"
                    + "]")
    public int sMembers = 2;

    public int joinFieldIdxR;
    public int joinFieldIdxS;
    public Long winR;
    public Long winS;

    public boolean processArgs(String[] args) throws Exception {
        if (!parseArgs(args))
            return false;

        if (help) {
            printHelp(System.out);
        }
        else {
            sanityCheckArgs();
            deriveArgs();
        }

        return true;
    }

    private boolean parseArgs(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        parser.getProperties().withUsageWidth(SCREEN_WIDTH);

        try {
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println();
            printHelp(System.err);

            return false;
        }

        return true;
    }

    private void sanityCheckArgs() {
        joinFieldR = joinFieldR.trim();
        checkState(schemaR.contains(joinFieldR),
                "Schema R does not contain field " + joinFieldR);

        joinFieldS = joinFieldS.trim();
        checkState(schemaS.contains(joinFieldS),
                "Schema S does not contain field " + joinFieldS);

        operator = operator.trim();
        checkState(
                ImmutableList.of("=", "!=", ">", ">=", "<", "<=").contains(
                        operator), "Invalid join operator: " + operator);

        checkState(numWorkers > 0, "At least one worker is required");
//        checkState(numGenerators >= 0,
//                "Non-negative number of sources R&S is required");
//        checkState(numGeneratorsR >= 0,
//                "Non-negative number of sources R is required");
//        checkState(numGeneratorsS >= 0,
//                "Non-negative number of sources S is required");
        checkState(numShufflers > 0, "At least one shuffler is required");
        checkState(numReshufflers > 0, "At least one reshuffler is required");
        checkState(numPartitionsR > 0, "At least one partition R is required");
        checkState(numPartitionsS > 0, "At least one partition S is required");
        checkState(numGatherers > 0, "At least one result gatherer is required");

        checkState(tuplesPerSecond >= 0, "TPS of source R&S cannot be negative");
        checkState(tuplesPerSecondR >= 0, "TPS of source R cannot be negative");
        checkState(tuplesPerSecondS >= 0, "TPS of source S cannot be negative");

        checkState(subindexSize > 0, "Size of each sub-index must be positive");

        checkState(!window || winInSecR > 0,
                "Window size for stream R must be positive");
        checkState(!window || winInSecS > 0,
                "Window size for stream S must be positive");

        checkState(noDedup || dedupSize > 0, "Dedup set size must be positive");

        checkState(aggReportInSeconds > 0,
                "Aggregation report interval must be at least 1 second");

        checkState(intUpper > intLower);
        checkState(doubleUpper > doubleLower);
        checkState(charsLength > 0, "length of chars must be positive");
    }

    private void deriveArgs() {
        if (outputDir != null) {
            outputDir = normalizeNoEndSeparator(outputDir);
            if (outputDir.isEmpty()) {
                outputDir = ".";
            }
            else {
                outputDir += "_" + getTimestamp();
            }
        }

        joinFieldIdxR = getFieldIdx(schemaR, joinFieldR);
        joinFieldIdxS = getFieldIdx(schemaS, joinFieldS);

        winR = (long) (winInSecR * 1000);
        winS = (long) (winInSecS * 1000);
    }

    public void showArgs() {
        for (String msg : getInfo()) {
            System.out.println(msg);
        }
    }

    public void logArgs() {
        for (String msg : getInfo()) {
            LOG.info(msg);
        }
    }

    public void logArgs(FileWriter output) {
        for (String msg : getInfo()) {
            output.write(msg);
        }
    }

    private List<String> getInfo() {
        List<String> info = newLinkedList();

        info.add("  schema R: [" + schemaR + "];  join_field_index: "
                + joinFieldIdxR);
        info.add("  schema S: [" + schemaS + "];  join_field_index: "
                + joinFieldIdxS);
        info.add("  join predicate: R." + joinFieldR + " " + operator + " S."
                + joinFieldS);

        info.add("  topology name: " + topologyName);
        info.add("  remote mode: " + remoteMode);
        info.add("  local runtime: " + localRuntime + " sec");
        info.add("  debug: " + debug);

        info.add("  # workers: " + numWorkers);

        info.add("  # shufflers: " + numShufflers);
        info.add("  # reshufflers: " + numReshufflers);
        info.add("  # partitions R: " + numPartitionsR);
        info.add("  # partitions S: " + numPartitionsS);
        info.add("  # result gatherers: " + numGatherers);

        info.add("  tps of each source R&S: " + tuplesPerSecond);
        info.add("  tps of each source R: " + tuplesPerSecondR);
        info.add("  tps of each source S: " + tuplesPerSecondS);
        info.add("  fluctuation: " + fluctuation);

        info.add("  size of each sub-index: " + subindexSize);

        info.add("  sliding window: " + window);
        info.add("  window size for R: " + winR + " ms");
        info.add("  window size for S: " + winS + " ms");
        info.add("  sub-index size: " + subindexSize + " tuples");

        info.add("  deduplication: " + !noDedup);
        info.add("  dedup set size: " + dedupSize + " items");

        info.add("  aggregation: " + aggregate);
        info.add("  aggregation report interval: " + aggReportInSeconds
                + " sec");

        info.add("  output: " + !noOutput);
        if (outputDir != null) {
            info.add("  output dir: " + outputDir);
        }
        info.add("  simple output: " + simple);

        return info;
    }

    private void printHelp(PrintStream out) {
        String cmd = "java " + _mainclassName;
        String cmdIndent = "  " + cmd;

        out.println("USAGE: ");
        out.println(cmdIndent + " [OPTION]...");
        out.println();
        out.println("OPTIONS: ");
        (new CmdLineParser(this)).printUsage(out);
        out.println();
        out.println("EXAMPLES: ");
        out.println(cmdIndent + " --remote --name MyTopology");
        out.println(cmdIndent + " -r 2 -tr 50 -s 2 -ts 50 -rs 0 -fluc");
        out.println(cmdIndent + " -n 4 -f1 2 -f2 2 -pr 4 -ps 4 -g 4");
        out.println();
    }

    public static int main(String[] args) throws Exception {
        TopologyArgs allArgs = new TopologyArgs("TopologyArgs");

        if (!allArgs.processArgs(args))
            return -1;

        if (!allArgs.help)
            allArgs.showArgs();

        return 0;
    }
}