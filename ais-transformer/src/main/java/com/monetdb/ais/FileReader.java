package com.monetdb.ais;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.io.Files;

public class FileReader {
    public static String readFile(String path) {
        List<String> content = new ArrayList<>();
        try {
            content = Files.readLines(new File(path), Charset.defaultCharset());
        }catch(IOException e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        String result = content.stream()
            .map(n -> String.valueOf(n)) 
            .collect(Collectors.joining("\n"));

        return result;
    } 
}
