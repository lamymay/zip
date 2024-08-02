package com.arc.zero.controller.data.file.split;

import com.arc.core.util.FileUtil;
import com.arc.core.util.JSON;

import java.io.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileMerger {

    /**
     * 将指定目录下的小文件合并为一个大文件
     *
     * @param inputDir       包含小文件的目录
     * @param outputFileName 合并后的大文件名称
     * @throws IOException
     */
    public static void mergeFiles(File inputDir, String outputFileName) throws IOException {
        File[] files = inputDir.listFiles();
        if (files == null || files.length == 0) {
            throw new FileNotFoundException("No files found in the directory.");
        }

        // 正则表达式用于匹配文件名模式
        Pattern pattern = Pattern.compile(".*\\.part(\\d+)$");
        File[] sortedFiles = files;
        // 排序文件以确保按顺序合并
        Arrays.sort(sortedFiles, (f1, f2) -> {
            Matcher m1 = pattern.matcher(f1.getName());
            Matcher m2 = pattern.matcher(f2.getName());
            if (m1.matches() && m2.matches()) {
                return Integer.compare(Integer.parseInt(m1.group(1)), Integer.parseInt(m2.group(1)));
            }
            return 0;
        });

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFileName))) {
            byte[] buffer = new byte[1024];
            for (File file : sortedFiles) {
                Matcher matcher = pattern.matcher(file.getName());
                if (matcher.matches()) {
                    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                        int bytesRead;
                        while ((bytesRead = bis.read(buffer)) != -1) {
                            bos.write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        File inputDir = new File("D:\\新建文件夹\\output");  // 包含小文件的目录
        String outputFileName = "D:\\新建文件夹\\孙露 - 光阴的故事V2.wav";  // 合并后的大文件名称
        mergeFiles(inputDir, outputFileName);
        System.out.println("Files merged into " + outputFileName);

        String[] hashArray1 = FileSameCheckTool.calculateHash(new File("D:\\新建文件夹\\孙露 - 光阴的故事.wav"));
        String[] hashArray2 = FileSameCheckTool.calculateHash(new File("D:\\新建文件夹\\孙露 - 光阴的故事V2.wav"));
        System.out.println(JSON.toJSONString(hashArray1));
        System.out.println(JSON.toJSONString(hashArray2));
    }
}
