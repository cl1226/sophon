package com.scistor.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestPython {

    public static void main(String[] args) throws InterruptedException, IOException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("python", "C:\\Users\\Mary\\Desktop\\test.py");
        Process process = builder.start();
        process.waitFor();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        bufferedReader.lines().forEach(System.out::println);
    }

}
