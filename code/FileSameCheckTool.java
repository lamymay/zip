package com.arc.zero.controller.data.file.split;


import com.arc.core.util.FileUtil;
import com.arc.core.util.JSON;
import com.arc.core.util.StringUtil;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class FileSameCheckTool {

    private static final Logger log = LoggerFactory.getLogger(FileSameCheckTool.class);

    //public static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static File tmpFile;
    //    static int max_file_size = 500 * 1024 * 1024;//设置最大允许大小 500MB
    static int max_file_size = 20 * 1024 * 1024 * 1024 * 1024;//设置最大允许大小

    static {
        try {
//            final String tmpFilePath = System.getProperty("java.io.tmpdir") + File.separator + "image_backup";
            String osName = System.getProperty("os.name") == null ? "" : System.getProperty("os.name");
            if (osName.toUpperCase().contains("WINDOWS")) {
                tmpFile = new File("/del");
            } else if (osName.toUpperCase().contains("MAC")) {
                String userHome = System.getProperty("user.home");//用户的主目录System.out.println("userHome:   "+userHome);
                //String userDir = System.getProperty("user.dir");//项目的当前工作目录
                tmpFile = new File(userHome + File.separator + "Desktop/backup");

            } else {
                throw new RuntimeException("临时目录不存在");
            }

            if (tmpFile.exists()) {
                System.out.println("\033[35;4m" + "程序尝试创建输出目录:\t" + tmpFile + "\n\033[0m");

            } else {
                boolean mkdirs = tmpFile.mkdirs();
                System.out.println("\033[35;4m" + "程序尝试创建输出目录,创建结果" + (mkdirs ? "成功" : "失败") + "\n" + tmpFile + "\n\033[0m");


            }

        } catch (Exception exception) {
            log.error("FileSameCheckTool static ERROR", exception);
            System.err.println("tmpFile配置错误，程序设置临时目录为:" + tmpFile);
        }


    }

    // 计算文件的SHA-256哈希值
    public static String[] calculateHash(File file) {
        try {
            MessageDigest digestAES256 = MessageDigest.getInstance("SHA-256");
            MessageDigest digestMD5 = MessageDigest.getInstance("MD5");
            try (InputStream is = new FileInputStream(file)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    digestAES256.update(buffer, 0, bytesRead);
                    digestMD5.update(buffer, 0, bytesRead);
                }
            }
            return new String[]{bytesToHex(digestAES256.digest()), bytesToHex(digestMD5.digest())};

        } catch (Exception exception) {
            log.error("Error calculateHash", exception);
            return new String[]{};
        }
    }

    public static String calculateSHA256(String content) {
        try {
            MessageDigest digestSHA256 = MessageDigest.getInstance("SHA-256");
            try (InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    digestSHA256.update(buffer, 0, bytesRead);
                }
            }
            return bytesToHex(digestSHA256.digest());

        } catch (Exception exception) {
            log.error("Error calculateHash", exception);
            return null;
        }
    }

    public static String calculateSHA256(byte[] content) {
        try {
            MessageDigest digestSHA256 = MessageDigest.getInstance("SHA-256");
            try (InputStream is = new ByteArrayInputStream(content)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    digestSHA256.update(buffer, 0, bytesRead);
                }
            }
            return bytesToHex(digestSHA256.digest());

        } catch (Exception exception) {
            log.error("Error calculateHash", exception);
            return null;
        }
    }

    public static void main(String[] args) {
        String sha256_1 = calculateSHA256(FileUtil.readLinesAsString(new File("E:\\syncd\\扫描结果JSON\\_舟山-东极海运.txt")));
        String sha256_2 = calculateSHA256(FileUtil.readLinesAsString(new File("E:\\syncd\\扫描结果JSON\\_舟山-东极海运 - 副本.txt")));
        System.out.println(sha256_2.equals(sha256_1));
        System.out.println(sha256_1);
        System.out.println(sha256_2);
    }

//
//    public static String calculateHashSHA256(File file) {
//        try {
//            return calculateHashSHA256Closeable(new FileInputStream(file));
//        } catch (Exception exception) {
//            log.error("Error calculateHashSHA256", exception);
//            return null;
//        }
//    }

//    public static String calculateHashSHA256Closeable(InputStream inputStream) {
//        if (inputStream == null) return null;
//        try {
//            MessageDigest digest = MessageDigest.getInstance("SHA-256");
//            byte[] buffer = new byte[8192];
//            int bytesRead;
//            while ((bytesRead = inputStream.read(buffer)) != -1) {
//                digest.update(buffer, 0, bytesRead);
//            }
//            return bytesToHex(digest.digest());
//
//        } catch (Exception exception) {
//            log.error("Error calculateHash", exception);
//            return null;
//        } finally {
//            try {
//                inputStream.close();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    // 将字节数组转换为十六进制字符串
    public static String bytesToHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte b : bytes) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }


    //遍历指定目录下的所有文件
    public static List<File> scanAllFiles(File sourceDir) {
        final long scanAllFilesBefore = System.currentTimeMillis();

        // 遍历源目录中文件
        List<File> pathList = new LinkedList<>();
        try {
            pathList = Files.walk(sourceDir.toPath()).map(it -> it.toFile()).filter(File::isFile)
                    //.filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        } catch (IOException exception) {
            log.error("scanAllFiles ", exception);
        }

        final int totalFiles = pathList.size();
        final long scanAllFilesAfter = System.currentTimeMillis();
        System.out.printf("目录扫描完毕，总数量%s,耗时=%s%n", totalFiles, StringUtil.displayTimeWithUnit(scanAllFilesBefore, scanAllFilesAfter));


        return pathList;
    }


    public static Map<String, List<File>> scanFileMapParallel(File sourceDir) {
        long scanFileMapParallelStart = System.currentTimeMillis();

        FileUtil.requireFileDirectoryExistsOrElseThrows(sourceDir);
        List<File> files = scanAllFiles(sourceDir);
        final int fileCountTotal = files.size();

        long fileLengthCountStart = System.currentTimeMillis();
        long fileLengthTotal = FileUtil.sizeOf(sourceDir);
        long fileLengthCountEnd = System.currentTimeMillis();

        // 创建ListeningExecutorService  ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        ListeningExecutorService executorService = com.google.common.util.concurrent.MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(6));

        // 提交异步任务并注册回调函数
        List<ListenableFuture<FileDTO>> futures = new ArrayList<>();
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicLong fileLengthCount = new AtomicLong(0);

        ConcurrentHashMap<String, List<File>> fileMap = new ConcurrentHashMap<>(fileCountTotal);

        final long submitBefore = System.currentTimeMillis();

        for (File file : files) {
            futures.add(executorService.submit(new Callable<FileDTO>() {

                @Override
                public FileDTO call() throws Exception {
                    final long everyTaskStart = System.currentTimeMillis();
                    // 进度 ++
                    int processFileCountSoFar = processedCount.incrementAndGet();
                    long processFileLengthSoFar = fileLengthCount.addAndGet(file.length());

                    String[] hashArray = calculateHash(file);

                    if (hashArray.length != 2 || StringUtils.isBlank(hashArray[0]) || StringUtils.isBlank(hashArray[1])) {
                        String errorMessage = "跳过,原因:hash计算异常,file=" + file;
                        return new FileDTO(file, false, errorMessage);
                    }


                    // debug 打印
                    if (processFileCountSoFar % 100 == 0) {

                        String countRate = String.format("%.2f ", (float) processFileCountSoFar / fileCountTotal);
                        String lengthRate = doCalculateRate(processFileLengthSoFar, fileLengthTotal);

                        final long everyTaskCalculateHashEnd = System.currentTimeMillis();
                        long costSoFar = (everyTaskCalculateHashEnd - scanFileMapParallelStart);
                        long costSoFarSecond = costSoFar / 1000;
                        if (costSoFarSecond < 1) costSoFarSecond = 1;

                        Map<String, Object> debugMap = new TreeMap<>();
                        debugMap.put("工作线程", Thread.currentThread().getName());
                        debugMap.put("扫描单个文件耗时", StringUtil.displayTimeWithUnit(everyTaskCalculateHashEnd, everyTaskStart));
                        debugMap.put("扫描迄今已经耗时", StringUtil.displayTimeWithUnit(costSoFar));
                        debugMap.put("数量维度-扫描速度百分比,", String.format("%s/%s=%s", processFileCountSoFar, fileCountTotal, countRate));
                        debugMap.put("容量维度-扫描速度百分比,", String.format("%s/%s=%s", processFileLengthSoFar, fileLengthTotal, lengthRate));
                        debugMap.put("扫描文件速度: %s个/秒", doCalculateRate(processFileCountSoFar, costSoFarSecond));
                        debugMap.put("扫描磁盘速度: %sMB/S", doCalculateRate(FileUtil.formatFileLengthUseMegabytesUnit(processFileLengthSoFar), costSoFarSecond));
                        printDebugMap(debugMap);
                    }

                    return new FileDTO(file, true, "OK", hashArray);

                }


            }));
        }
        final long submitTaskOK = System.currentTimeMillis();


        final long handleFutureBefore = System.currentTimeMillis();
        try {

            // 使用Futures类的allAsList方法等待所有任务完成
            ListenableFuture<List<FileDTO>> allFutures = Futures.allAsList(futures);

            // 阻塞等待所有任务完成
            //这样，allAsList 方法将等待所有任务完成，然后你可以从 results 列表中获取各个任务的结果。确保在异常情况下处理异常，以便在出现问题时进行适当的处理。
            List<FileDTO> results = allFutures.get(24, TimeUnit.HOURS);
            for (FileDTO fileDTO : results) {
                if (fileDTO == null) {
                    continue;
                }
                File file = fileDTO.getFile();
                if (fileDTO.isDoNextStep()) {
//                    List<File> tempFileList = fileMap.get(fileDTO.getHashAES256());
//                    if (tempFileList == null) tempFileList = new LinkedList<>();
                    List<File> tempFileList = fileMap.computeIfAbsent(fileDTO.getHashAES256(), k -> new LinkedList<>());
                    tempFileList.add(file);
                } else {
                    System.out.println(fileDTO.getMessage() + "  " + file);
                }
            }

            //  executorService.shutdown(); //主线程等待线程池任务跑完 关闭线程池 此处可能会复用建议不关闭
        } catch (Exception exception) {
            // 处理异常
            log.error("获取异步处理结果异常", exception);
        }
        final long handleFutureAfter = System.currentTimeMillis();


        printPlanAsync(fileMap, sourceDir);
        System.out.println("第一次统计目录总大小，耗时=" + StringUtil.displayTimeWithUnit(fileLengthCountStart, fileLengthCountEnd));
        System.out.println("提交任务到线程池完毕，耗时=" + StringUtil.displayTimeWithUnit(submitBefore, submitTaskOK));
        System.out.println("处理异步任务结果，耗时=" + StringUtil.displayTimeWithUnit(handleFutureBefore, handleFutureAfter));
        System.out.println("scanFileMapParallel 耗时=" + StringUtil.getTimeStringSoFar(scanFileMapParallelStart));

        return fileMap;


    }

    private static String doCalculateRate(int processFileLengthSoFar, long fileLengthTotal) {
        return String.format("%.2f ", (float) processFileLengthSoFar / fileLengthTotal);
    }

    private static String doCalculateRate(long processFileLengthSoFar, long fileLengthTotal) {
        return String.format("%.2f ", (float) processFileLengthSoFar / fileLengthTotal);
    }

    private static String doCalculateRate(double processFileLengthSoFar, long fileLengthTotal) {
        return String.format("%.2f", (float) processFileLengthSoFar / fileLengthTotal);
    }


    private static void printPlanAsync(Map<String, List<File>> fileMap, File sourceDir) {
        System.out.println(fileMap.size());

        final long sizeOfBefore = System.currentTimeMillis();
        long length = FileUtil.sizeOf(sourceDir); //单位应该是 B
        final long sizeOfAfter = System.currentTimeMillis();
        System.out.println("目录中文件大小: " + FileUtil.formatFileLengthWithUnit(length) + " 耗时:" + StringUtil.displayTimeWithUnit(sizeOfAfter, sizeOfBefore));

        int totalCount = 0;
        int needDeleteCount = 0;
        int sameHashFileBatch = 0;
        Map<String, Object> plan = new HashMap<>();

        if (fileMap != null && fileMap.size() > 0) {
            for (List<File> fileList : fileMap.values()) {
                if (fileList != null) {
                    totalCount = totalCount + fileList.size();
                    if (fileList.size() > 1) {
                        sameHashFileBatch = sameHashFileBatch + 1;
                        needDeleteCount = needDeleteCount + (fileList.size() - 1);
                    }

                }
            }
        }
        plan.put("sameHashFileBatch", sameHashFileBatch);
        plan.put("needDeleteCount", needDeleteCount);
        plan.put("totalCount", totalCount);
        log.info("\n#####################################################\n计算执行计划 \n#####################################################\n ");
        printDebugMap(plan);

    }

    private static void printDebugMap(Map<String, Object> debugMap) {
        if (debugMap == null) {
            System.out.println("null");
        }
        if (debugMap.isEmpty()) {
            System.out.println("[]");
        }

        StringBuilder appender = new StringBuilder(1000);
        for (Map.Entry<String, Object> entry : debugMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                String template = entry.getKey();
                if (template.contains("%s")) {
                    appender.append(String.format(template, value)).append(" ");

                } else {
                    appender.append(entry.getKey()).append(value).append(" ");
                }
            } else {
                appender.append(entry.getKey()).append(JSON.toJSONString(value)).append(" ");
            }
        }


        System.out.println(appender);


    }


    //检查文件是否需要跳过处理
    @Deprecated
    public static boolean needSkipFile(File file) {

        if (file == null) {
            System.err.println("null");
            return true;
        }
        if (file.isDirectory()) {
            System.out.println("\033[35;4m " + file.getPath() + "\t跳过处理,原因文件是目录!\033[0m");
            return true;
        }

        if (file.getName().contains("DS_Store")) {
            FileUtil.deleteFile(file, false);
            return true;
        }
        try {
            File outputDir = FileUtil.requireFileDirectoryExistsOrElseTryCreate(tmpFile);
            if (file.length() < 100 || file.getName().startsWith(".") || file.getName().endsWith(".")) {
                System.out.println("\033[13;4m 跳过处理的文件 " + file.getPath() + "\033[0m");
                FileUtil.deleteFile(file, true);
                // 移动文件到新目录
//                File renamedFile = FileUtil.detectionTargetFileV2(file, outputDir, ""); FileUtil.renameTo(file, renamedFile, 1);
                return true;
            }

            //
            if (file.length() > max_file_size) {
                File renamedFile = FileUtil.detectionTargetFileV2(file, outputDir, "");
                // 移动文件到新目录
                FileUtil.renameTo(file, renamedFile, 1);
                return true;
            }
        } catch (Exception exception) {
            log.error("检查文件是否需要跳过处理出现错误(Error when needSkipFile)" + file, exception);
            return true;
        }


        return false;
    }

    public static int countTotal(Map<String, ? extends Collection<?>> hashToFileListMap) {
        if (hashToFileListMap == null) return 0;

        int totalFiles = 0;
        for (Map.Entry<String, ? extends Collection<?>> hashToFiles : hashToFileListMap.entrySet()) {
            if (hashToFiles == null || hashToFiles.getValue() == null || hashToFiles.getValue().isEmpty()) {
                continue;
            }
            int count = hashToFiles.getValue().size();
            totalFiles = totalFiles + count;
        }

        return totalFiles;
    }

    private static LinkedList<File> listAndFilter(File source, List<String> allowFileTypes) {
        List<File> files = FileUtil.listFileByDir(source);

        final LinkedList<File> supportFileList = new LinkedList<>();
        for (File file : files) {
            final String extension = FileUtil.getExtensionWithTry(file, "").toLowerCase(Locale.ROOT);
            if (allowFileTypes.contains(extension)) {
                supportFileList.add(file);
            }
        }
        return supportFileList;
    }


    @Deprecated//未经过测试
    public static void compareEveryByte(String[] args) {
        FileInputStream file1 = null;
        FileInputStream file2 = null;

        if (args.length != 2) {
            System.out.println("The command line should be: java IOOperation testX.txt testX.txt");
            System.out.println("X should be one of the array: 1, 2, 3");
            System.exit(0);
        }

        try {
            file1 = new FileInputStream(args[0]);
            file2 = new FileInputStream(args[1]);

            try {

                if (file1.available() != file2.available()) {
                    //长度不同内容肯定不同
                    System.out.println(args[0] + " is not equal to " + args[1]);
                } else {
                    boolean tag = true;

                    while (file1.read() != -1 && file2.read() != -1) {
                        if (file1.read() != file2.read()) {
                            tag = false;
                            break;
                        }
                    }

                    if (tag)
                        System.out.println(args[0] + " equals to " + args[1]);
                    else
                        System.out.println(args[0] + " is not equal to " + args[1]);
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File can't find..");
        } finally {

            try {
                if (file1 != null)
                    file1.close();
                if (file2 != null)
                    file2.close();
            } catch (IOException e) {
                log.error("error ", e);
            }
        }
    }


    /**
     * 实现高效的文件内容比较工具函数。
     * 参考了网上的一些方法并做了改进。  测试发现使用MD5方式比较是不完备的，如果文件只改动一个字节，比如 本来数字“1”改成数字“2”，是无法正确比较的。
     * 所以还是采用了读取所有字节进行比较的方式比较靠谱。读取文件内容是的buffer大小会影响执行效率。对于10K级别的文本文件，经测试在10MS以内比较完成。
     * <p>
     * compare two file's content. if not equal then return false; else return true;
     * </p>
     */
    public static boolean isFileContentEqual(String oldFilePath, String newFilePath) {
        //check does the two file exist.
        if (StringUtils.isNotBlank(oldFilePath) && StringUtils.isNotBlank(newFilePath)) {
            File oldFile = new File(oldFilePath);
            File newFile = new File(newFilePath);
            FileInputStream oldInStream = null;
            FileInputStream newInStream = null;
            try {
                oldInStream = new FileInputStream(oldFile);
                newInStream = new FileInputStream(newFile);

                int oldStreamLen = oldInStream.available();
                int newStreamLen = newInStream.available();
                //check the file size first.
                if (oldStreamLen > 0 && oldStreamLen == newStreamLen) {
                    //read file data with a buffer.
                    int cacheSize = 128;
                    byte[] data1 = new byte[cacheSize];
                    byte[] data2 = new byte[cacheSize];
                    do {
                        int readSize = oldInStream.read(data1);
                        newInStream.read(data2);

                        for (int i = 0; i < cacheSize; i++) {
                            if (data1[i] != data2[i]) {
                                return false;
                            }
                        }
                        if (readSize == -1) {
                            break;
                        }
                    } while (true);
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //release the stream resource.
                try {
                    if (oldInStream != null)
                        oldInStream.close();
                    if (newInStream != null)
                        newInStream.close();
                } catch (IOException exception) {
                    log.error("Error when isFileContentEqual ", exception);

                }

            }
        }

        return false;
    }

    public static class FileDTO {

        private File file;

        /**
         * true=标识文件处理逻辑无异常可以继续处理,计算文件hash值是否成功
         */
        private boolean doNextStep;


        /**
         * 错误消息
         */
        private String message;
        private String hashMD5;
        private String hashAES256;
        private String[] hashArray;


        public FileDTO(File file, boolean doNextStep, String message) {
            this.file = file;
            this.doNextStep = doNextStep;
            this.message = message;
        }

        public FileDTO(File file, boolean doNextStep, String message, String[] hashArray) {
            this.file = file;
            this.doNextStep = doNextStep;
            this.message = message;
            this.hashArray = hashArray;
            if (hashArray != null && hashArray.length == 2) {
                this.hashAES256 = hashArray[0];
                this.hashMD5 = hashArray[1];
            }
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public boolean isDoNextStep() {
            return doNextStep;
        }

        public void setDoNextStep(boolean doNextStep) {
            this.doNextStep = doNextStep;
        }

        public String getHashMD5() {
            return hashMD5;
        }

        public void setHashMD5(String hashMD5) {
            this.hashMD5 = hashMD5;
        }

        public String getHashAES256() {
            return hashAES256;
        }

        public void setHashAES256(String hashAES256) {
            this.hashAES256 = hashAES256;
        }

        public String[] getHashArray() {
            return hashArray;
        }

        public void setHashArray(String[] hashArray) {
            this.hashArray = hashArray;
        }
    }
}


//     double humanNumber = 0D;
//        String humanNumberUnit;
//        if (length < kByte) {
//            humanNumber = length;
//            humanNumberUnit = "B";
//        } else if (length < mByte) {
//            humanNumber = length / kByte;
//            humanNumberUnit = "KB";
//        } else if (length < pByte) {
//            humanNumber = length / mByte;
//            humanNumberUnit = "MB";
//
//        } else {
//            throw new RuntimeException("错误,容量数字不是日常可见的数量级!");
//        }
//        // %.2f %. 表示 小数点前任意位数 2 表示两位小数 格式后的结果为f 表示浮点型。

/*


        double kByte = 1024;
        double mByte = 1024 * kByte;
        double gByte = 1024 * mByte;
        double tByte = 1024 * gByte;
        double eByte = 1024 * tByte;
        double pByte = 1024 * eByte;

        double humanNumber = 0D;
        String humanNumberUnit;
        if (length < kByte) {
            humanNumber = length;
            humanNumberUnit = "B";
        } else if (length < mByte) {
            humanNumber = length / kByte;
            humanNumberUnit = "KB";
        } else if (length < pByte) {
            humanNumber = length / mByte;
            humanNumberUnit = "MB";
//        } else if (length < tByte) {
//            humanNumber = length / gByte;
//            humanNumberUnit = "GB";
//        } else if (length < eByte) {
//            humanNumber = length / tByte;
//            humanNumberUnit = "TB";
//        } else if (length < pByte) {
//            humanNumber = length / eByte;
//            humanNumberUnit = "TB";
        } else {
            throw new RuntimeException("错误,容量数字不是日常可见的数量级!");
        }
* */