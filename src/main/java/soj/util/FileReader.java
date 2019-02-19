package soj.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

//import static org.apache.commons.io.FilenameUtils.normalize;
import org.slf4j.Logger;

import static org.apache.commons.io.FilenameUtils.normalize;
import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.base.Splitter;
import static com.google.common.base.Charsets.UTF_8;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Files.newReader;

public class FileReader
{
    private static final String DEFAULT_SPLIT_PATTERN = "\\s+";
    private static final Logger LOG = getLogger(FileReader.class);

    private final String _filename;
    private final BufferedReader _reader;
    private final Splitter _splitter;

    public FileReader(String filename, String splitPunc) throws Exception {
        _filename = normalize(filename);
        _reader = getReader();

        String splitePattern;
        if (splitPunc == null || splitPunc.trim().isEmpty()) {
            splitePattern = DEFAULT_SPLIT_PATTERN;
        }
        else {
            splitePattern = splitPunc.trim() + "\\s+";
        }
        _splitter = Splitter.on(Pattern.compile(splitePattern)).trimResults();
    }

    private BufferedReader getReader() throws Exception {
        if (_filename == null || _filename.isEmpty()) {
            LOG.debug("No input file is specified");
            return null;
        }

        File file = new File(_filename);
        if (!file.exists()) {
            LOG.error("File not exists: " + _filename);
            return null;
        }
        else {
            return newReader(file, UTF_8);
        }
    }

    public String readLine() throws IOException {
        if (_reader == null) {
            return null;
        }
        else {
            return _reader.readLine();
        }
    }

    public List<String> readLineAndSplit() throws IOException {
        String line = readLine();

        if (line == null)
            return null;
        else
            return newArrayList(_splitter.split(line));
    }

    public String getFilename() {
        return _filename;
    }
}