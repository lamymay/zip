package com.arc.zero.controller.data.file.split;

import java.io.*;

public class FileSplitter {

    /**
     * 将大文件切割为指定数量的小文件
     * @param file 要切割的文件
     * @param numberOfParts 切割的小文件数量
     * @param outputDir 输出目录
     * @throws IOException
     */
    public static void splitFile(File file, int numberOfParts, File outputDir) throws IOException {
        if (!outputDir.exists()) {
            outputDir.mkdirs();  // 创建输出目录
        }

        long fileSize = file.length();
        long partSize = fileSize / numberOfParts;
        long remainingBytes = fileSize;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            int partNumber = 0;
            while (remainingBytes > 0) {
                File partFile = new File(outputDir, file.getName() + ".part" + partNumber);
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(partFile))) {
                    long bytesToWrite = Math.min(partSize, remainingBytes);
                    while (bytesToWrite > 0 && (bytesRead = bis.read(buffer, 0, (int) Math.min(buffer.length, bytesToWrite))) != -1) {
                        bos.write(buffer, 0, bytesRead);
                        bytesToWrite -= bytesRead;
                        remainingBytes -= bytesRead;
                    }
                }
                partNumber++;
            }
        }
    }

    public static void main(String[] args) {
        try {
            File file = new File("H:\\app\\Anaconda3-2024.06-1-Windows-x86_64.exe");

            int numberOfParts = 100;  // 要分割的文件个数
            File outputDir = new File("D:\\fast\\zip\\02exe");  // 输出目录
            splitFile(file, numberOfParts, outputDir);
            System.out.println("File split into " + numberOfParts + " parts.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
