package com.monetdb.ais;

import java.io.FileWriter;
import java.util.List;

public class NewFileWriter implements IOutputWriter {
    private String path;


    private String[] fileNames = {"ais-msgs1", "ais-msgs4", "ais-msgs5", "ais-msgs9", "ais-msgs18", "ais-msgs21", "ais-msgs24" };

    //CSV Header for types 1,2 and 3
    private final String headermsgpos = "mmsi,lon,lat,nav_status,turn,speed,course,heading,timestamp";
    //CSV Header for type 4
    private final String headermsg4 = "mmsi,lon,lat,timestamp";
    //CSV Header for type 5
    private final String headermsg5 = "mmsi,name,imo,call_sign,ship_type,destination,draught,to_bow,to_stern,to_port,to_starboard,timestamp";
    //CSV Header for type 9
    private final String headermsg9 = "mmsi,lon,lat,altitude,speed,course,timestamp";
    //CSV Header for type 18
    private final String headermsg18 = "mmsi,lon,lat,speed,course,heading,timestamp";
    //CSV Header for type 21
    private final String headermsg21 = "mmsi,aid_type,name,lon,lat,to_bow,to_stern,to_port,to_starboard,timestamp";
    //CSV Header for type 24
    private final String headermsg24 = "mmsi,name,call_sign,ship_type,to_bow,to_stern,to_port,to_starboard,timestamp";

    /**
     * Constructs a NewFileWriter.
     * Also creates the necessary files in the given path. 
     * @param path
     */
    public NewFileWriter(String path)  {
        this.path = path;

        this.CreateFiles(fileNames[0], this.headermsgpos);
        this.CreateFiles(fileNames[1], this.headermsg4);
        this.CreateFiles(fileNames[2], this.headermsg5);
        this.CreateFiles(fileNames[3], this.headermsg9);
        this.CreateFiles(fileNames[4], this.headermsg18);
        this.CreateFiles(fileNames[5], this.headermsg21);
        this.CreateFiles(fileNames[6], this.headermsg24);
    }

    public void Write(List<List<String>> messages) {
        if(messages.size() != fileNames.length) {
            return;
        }

        for(int i = 0; i < messages.size(); i++) {
            writeToCsvFile(fileNames[i], messages.get(i));
        }
    }

    private void CreateFiles(String fileName, String header) {
        this.writeToCsvFile(fileName, List.of(header));
    }

    private void writeToCsvFile(String fileName, List<String> input) {
        if(input.size() <= 0) {
            return;
        }

        try {
            FileWriter file = new FileWriter(this.path + "/" + fileName + ".csv");

            for(String s : input) {
                file.append(s);
                file.append("\n");
            }

            file.flush();
            file.close();
        } catch(Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }
}
