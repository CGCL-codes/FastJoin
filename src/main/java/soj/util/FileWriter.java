package soj.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

//import static org.apache.commons.io.FilenameUtils.getFullPath;
//import static org.apache.commons.io.FilenameUtils.normalize;
import org.slf4j.Logger;

import static org.apache.commons.io.FilenameUtils.getFullPath;
import static org.apache.commons.io.FilenameUtils.normalize;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Files.newWriter;

public class FileWriter
{
    private static final int DEFAULT_FLUSH_SIZE = 512;
    private static final Logger LOG = getLogger(FileWriter.class);

    private final String _filename;
    private final BufferedWriter _writer;

    private PrintStream _printer = null;

    private int _flushSize = DEFAULT_FLUSH_SIZE;
    private int _count = 0;

    public FileWriter(PrintStream ps) {
        _filename = null;
        _writer = null;
        _printer = ps;
    }

    public FileWriter(String filename, boolean overwrite) {
        _filename = normalize(filename);
        _writer = getWriter(overwrite);
    }

    public FileWriter(String filename) {
        _filename = normalize(filename);
        _writer = getWriter(false);
    }

    public FileWriter(String dir, String prefix, String suffix,
            boolean overwrite) {
        _filename = constructFilename(dir, prefix, suffix);
        _writer = getWriter(overwrite);
    }

    public FileWriter(String dir, String prefix, String suffix) {
        _filename = constructFilename(dir, prefix, suffix);
        _writer = getWriter(false);
    }

    private String constructFilename(String dir, String prefix, String suffix) {
        String filename;

        if (dir == null) {
            filename = null;
        }
        else {
            filename = (dir.isEmpty() ? "." : dir) + "/";

            if (prefix == null || prefix.isEmpty()) {
                filename += "";
            }
            else {
                filename += prefix + "_";
            }
            filename += TimeUtils.getTimestamp();

            filename += "." + suffix;
        }

        return normalize(filename);
    }

    private BufferedWriter getWriter(boolean overwrite) {
        if (_filename == null || _filename.isEmpty()) {
            LOG.debug("Dummy file writer is in use");
            return null;
        }

        mkdir(getFullPath(_filename));

        BufferedWriter writer;
        File file = new File(_filename);
        if (file.exists()) {
            if (overwrite) {
                file.delete();
                createNewFile(file);
                LOG.info("Overwriting existing file: " + _filename);
                writer = getWriter(file);
            }
            else {
                LOG.error("File exists (not overwriting): " + _filename);
                writer = null;
            }
        }
        else {
            createNewFile(file);
            LOG.debug("New file created: " + _filename);
            writer = getWriter(file);
        }

        return writer;
    }

    private BufferedWriter getWriter(File file) {
        BufferedWriter writer = null;
        try {
            writer = newWriter(file, UTF_8);
        }
        catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
        }

        return writer;
    }

    private void mkdir(String path) {
        File dir = new File(path);
        dir.mkdirs();
    }

    private void createNewFile(File file) {
        try {
            file.createNewFile();
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    public void write(String msg) {
        write(msg, false);
    }

    public void writeImmediately(String msg){
        write(msg, true);
    }

    private void write(String msg, boolean immediate){
        append(msg);
        flush(immediate);

        if (_printer != null) {
            _printer.println(msg);
        }
    }

    public void endOfFile() {
        write("EOF");
        flush(true);
    }

    private void append(String msg) {
        if (_writer != null) {
            try {
                _writer.append(msg + "\n");
            }
            catch (IOException e) {
                LOG.error(e.getMessage());
            }

            ++_count;
        }
    }

    private void flush(boolean immediate) {
        if (_writer != null) {
            if (immediate || (_count % _flushSize == 0)) {
                try {
                    _writer.flush();
                }
                catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    public String getFilename() {
        return _filename;
    }

    public boolean isNull() {
        return (_writer == null);
    }

    public FileWriter setFlushSize(int size) {
        _flushSize = size;
        return this;
    }

    public FileWriter setPrintStream(PrintStream ps) {
        _printer = ps;
        return this;
    }
}