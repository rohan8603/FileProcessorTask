import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class FileProcessorTask implements Callable<Map<String, Object>> {
    private final Path filePath;
    private final AtomicInteger processedCount;
    private final AtomicInteger failedCount;

    public FileProcessorTask(Path filePath, AtomicInteger processedCount, AtomicInteger failedCount) {
        this.filePath = filePath;
        this.processedCount = processedCount;
        this.failedCount = failedCount;
    }

    @Override
    public Map<String, Object> call() {
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();

        try {
            result.put("filename", filePath.getFileName().toString());
            result.put("thread", Thread.currentThread().getName());

            long fileSize = Files.size(filePath);
            result.put("size_bytes", fileSize);

            List<String> lines = Files.readAllLines(filePath);
            result.put("line_count", lines.size());

            long wordCount = lines.stream()
                    .flatMap(line -> Arrays.stream(line.split("\\s+")))
                    .filter(word -> !word.isEmpty())
                    .count();
            result.put("word_count", wordCount);

            if (!lines.isEmpty()) {
                String[] words = lines.get(0).split("\\s+");
                if (words.length > 0) result.put("sample_word", words[0]);
            }

            Thread.sleep(Math.min(fileSize / 1024, 100));

            result.put("processing_time_ms", System.currentTimeMillis() - startTime);
            result.put("status", "SUCCESS");

            processedCount.incrementAndGet();

        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("error", e.getMessage());
            result.put("processing_time_ms", System.currentTimeMillis() - startTime);
            failedCount.incrementAndGet();
        }

        return result;
    }
}

class FileProcessingManager {
    private final ExecutorService threadPool;
    private final BlockingQueue<Path> fileQueue;
    private final List<Future<Map<String, Object>>> futures;
    private final AtomicInteger processedFiles;
    private final AtomicInteger failedFiles;
    private final AtomicLong totalProcessingTime;
    private final AtomicLong totalFilesSize;

    public FileProcessingManager(int poolSize) {
        this.threadPool = Executors.newFixedThreadPool(poolSize);
        this.fileQueue = new LinkedBlockingQueue<>();
        this.futures = new CopyOnWriteArrayList<>();
        this.processedFiles = new AtomicInteger(0);
        this.failedFiles = new AtomicInteger(0);
        this.totalProcessingTime = new AtomicLong(0);
        this.totalFilesSize = new AtomicLong(0);
    }

    public void processDirectory(String directoryPath) throws IOException, InterruptedException {
        System.out.println("📁 Processing directory: " + directoryPath);

        List<Path> files = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            paths.filter(Files::isRegularFile)
                 .filter(p -> p.toString().endsWith(".txt") ||
                              p.toString().endsWith(".csv") ||
                              p.toString().endsWith(".log"))
                 .forEach(files::add);
        }

        System.out.println("Found " + files.size() + " files.");

        if (files.isEmpty()) return;

        fileQueue.addAll(files);
        startProcessing();
        awaitCompletion();
        generateReport();
    }

    private void startProcessing() {
        while (!fileQueue.isEmpty()) {
            Path file = fileQueue.poll();
            if (file != null) {
                FileProcessorTask task = new FileProcessorTask(file, processedFiles, failedFiles);
                Future<Map<String, Object>> future = threadPool.submit(task);
                futures.add(future);
            }
        }
    }

    private void awaitCompletion() throws InterruptedException {
        int totalTasks = futures.size();

        while (processedFiles.get() + failedFiles.get() < totalTasks) {
            int completed = processedFiles.get() + failedFiles.get();
            double percent = (double) completed / totalTasks * 100;
            System.out.printf("Progress: %d/%d (%.1f%%)%n", completed, totalTasks, percent);
            Thread.sleep(1000);
        }

        for (Future<Map<String, Object>> future : futures) {
            try {
                Map<String, Object> result = future.get();
                if ("SUCCESS".equals(result.get("status"))) {
                    totalProcessingTime.addAndGet((Long) result.get("processing_time_ms"));
                    totalFilesSize.addAndGet((Long) result.get("size_bytes"));
                }
            } catch (ExecutionException e) {
                System.out.println("Task failed: " + e.getCause().getMessage());
            }
        }
    }

    private void generateReport() {
        System.out.println("\n========== FILE PROCESSING REPORT ==========");
        System.out.println("Total files processed: " + processedFiles.get());
        System.out.println("Failed files: " + failedFiles.get());
        System.out.println("Success rate: " +
                String.format("%.1f%%",
                        (double) processedFiles.get() /
                        (processedFiles.get() + failedFiles.get()) * 100));

        System.out.println("Total processing time: " + totalProcessingTime.get() + " ms");
        System.out.println("Total file size: " + formatFileSize(totalFilesSize.get()));

        if (processedFiles.get() > 0) {
            System.out.println("Average processing time: " +
                    totalProcessingTime.get() / processedFiles.get() + " ms/file");
            System.out.println("Average file size: " +
                    formatFileSize(totalFilesSize.get() / processedFiles.get()));
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool;
        System.out.println("\nThread Pool Stats:");
        System.out.println("Pool size: " + executor.getPoolSize());
        System.out.println("Active threads: " + executor.getActiveCount());
        System.out.println("Completed tasks: " + executor.getCompletedTaskCount());
    }

    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }

    public void shutdown() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Thread pool shutdown complete.");
    }
}

class ProducerConsumerExample {
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10);

    class FileProducer implements Runnable {
        private final List<String> files;

        public FileProducer(List<String> files) {
            this.files = files;
        }

        @Override
        public void run() {
            try {
                for (String file : files) {
                    queue.put(file);
                    System.out.println("Producer added: " + file);
                    Thread.sleep(100);
                }
                queue.put("END");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    class FileConsumer implements Runnable {
        private final String name;
        private int processed = 0;

        public FileConsumer(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String file = queue.take();
                    if ("END".equals(file)) {
                        queue.put("END");
                        break;
                    }
                    processFile(file);
                    processed++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println(name + " processed " + processed + " files");
        }

        private void processFile(String file) throws InterruptedException {
            System.out.println(name + " processing: " + file);
            Thread.sleep(200);
        }
    }

    public void runExample() throws InterruptedException {
        List<String> files = Arrays.asList(
                "file1.txt", "file2.txt", "file3.txt", "file4.txt",
                "file5.txt", "file6.txt", "file7.txt", "file8.txt"
        );

        Thread producer = new Thread(new FileProducer(files), "Producer");
        Thread c1 = new Thread(new FileConsumer("Consumer-1"));
        Thread c2 = new Thread(new FileConsumer("Consumer-2"));
        Thread c3 = new Thread(new FileConsumer("Consumer-3"));

        producer.start();
        c1.start();
        c2.start();
        c3.start();

        producer.join();
        c1.join();
        c2.join();
        c3.join();

        System.out.println("\nProducer-Consumer example completed!");
    }
}

public class Main {
    public static void main(String[] args) throws Exception {
        FileProcessingManager manager = new FileProcessingManager(4);
        manager.processDirectory("files"); 
        manager.shutdown();

        ProducerConsumerExample example = new ProducerConsumerExample();
        example.runExample();
    }
}
