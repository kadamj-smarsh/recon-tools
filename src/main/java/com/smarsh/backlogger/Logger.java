package com.smarsh.backlogger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Writes every log line to both the console and a persistent run.log file
 * in the output directory, so there's a record after the run finishes and
 * not just whatever scrolled by in the terminal.
 */
public class Logger implements AutoCloseable {

    static final String LOG_FILE = "run.log";
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final PrintWriter fileWriter;

    Logger(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        this.fileWriter = new PrintWriter(Files.newBufferedWriter(outputDir.resolve(LOG_FILE),
            StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    synchronized void info(String message) {
        write(System.out, message);
    }

    synchronized void warn(String message) {
        write(System.err, message);
    }

    private void write(java.io.PrintStream console, String message) {
        String line = "[" + LocalDateTime.now().format(TS) + "] " + message;
        console.println(line);
        fileWriter.println(line);
        fileWriter.flush();
    }

    @Override
    public void close() {
        fileWriter.close();
    }
}
