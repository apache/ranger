/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.db.upgrade;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileDBForTest {
    private FileDBForTest() {
    }

    public static void deleteFileIfExists(String filename) {
        File file = new File(filename);
        if (file.exists()) {
            if (file.delete()) {
                System.out.println("File deleted successfully: " + filename);
            } else {
                System.out.println("Failed to delete the file: " + filename);
            }
        } else {
            System.out.println("File does not exist: " + filename);
        }
    }

    public static boolean tagExists(String filePath, String tagName) {
        createFileIfNotExists(filePath);
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] changeLogRow = line.split(",");
                if (changeLogRow[changeLogRow.length - 1].equals(tagName)) {
                    return true; // String found
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
        return false; // String not found
    }

    public static void tag(String filePath, String tagName) {
        createFileIfNotExists(filePath);
        if (isFileEmpty(filePath)) {
            List<String> lines = new ArrayList<>();
            lines.add(",,," + tagName);
            writeLines(filePath, lines, true);
            //writeLineToFile(filePath,",,,"+tagName);
        } else {
            try {
                readAndModifyLastLine(filePath, tagName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static List<String> readLines(String filePath) {
        createFileIfNotExists(filePath);
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lines;
    }

    public static void writeLines(String filePath, List<String> lines, boolean doAppend) {
        createFileIfNotExists(filePath);
        try (FileWriter writer = new FileWriter(filePath, doAppend)) { // 'true' enables appending
            for (String line : lines) {
                writer.write(line);
                writer.write("\n");
            }
            System.out.println("File=" + filePath + " Lines written to file= " + lines);
        } catch (IOException e) {
            System.err.println("Error appending to file: " + e.getMessage());
        }
    }

    public static void removeLinesAfterTag(String filePath, String tagName) throws Exception {
        createFileIfNotExists(filePath);
        int          lastLineWithTag = -1;
        List<String> lines           = readLines(filePath);
        for (int i = 0; i < lines.size(); i++) {
            String[] changeLogRow = lines.get(i).split(",");
            if (changeLogRow[changeLogRow.length - 1].equals(tagName)) {
                lastLineWithTag = i;
            }
        }
        System.out.println("lastLineWithTag " + tagName + " = " + lastLineWithTag);
        if (lastLineWithTag == -1) {
            throw new Exception("Couldn't find Tag " + tagName + " in file, can't remove lines");
        }
        lines = lines.subList(0, lastLineWithTag + 1);

        writeLines(filePath, lines, false);
    }

    private static boolean isFileEmpty(String filePath) {
        File file = new File(filePath);
        // Check if file exists and its size is 0
        return file.exists() && file.isFile() && file.length() == 0;
    }

    private static void createFileIfNotExists(String filePath) {
        File file = new File(filePath);
        try {
            // Check if the file already exists
            if (file.exists()) {
                System.out.println("File already exists: " + file.getAbsolutePath());
            } else {
                // Create the file if it does not exist
                if (file.createNewFile()) {
                    System.out.println("File created successfully: " + file.getAbsolutePath());
                } else {
                    System.out.println("Failed to create the file.");
                }
            }
        } catch (IOException e) {
            // Handle exceptions
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
    }

    private static void readAndModifyLastLine(String filePath, String tagName) throws Exception {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("File does not exist or is not valid: " + filePath);
        }

        // Read all lines into a list
        List<String> lines = readLines(filePath);

        if (lines.isEmpty()) {
            System.out.println("The file is empty. Nothing to modify.");
            return;
        }

        // Modify the last line
        int      lastIndex          = lines.size() - 1;
        String   lastLine           = lines.get(lastIndex);
        String[] lastLineComponents = lastLine.split(",");
        if (lastLineComponents.length == 4) {
            //replace existing tag
            lastLineComponents[3] = tagName;
            lastLine = String.join(",", lastLineComponents);
        } else if (lastLineComponents.length == 3) {
            //add new tag to last line
            lastLine += ("," + tagName);
        } else {
            throw new Exception("Unhandled scenario for test case. Modify the logic if changes made to sample changelog file");
        }

        lines.set(lastIndex, lastLine);

        // Write the modified content back to the file
        writeLines(filePath, lines, false);

        System.out.println("Successfully modified the last line.");
    }
}
