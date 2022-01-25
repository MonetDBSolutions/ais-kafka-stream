package com.monetdb.ais;

import java.time.*;
import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.math.RoundingMode;

//Data format for decoded records
class Record {
    //General
    int type;
    int id;
    String timestamp;
    long offset;

    //Position
    double lat;
    double log;
    int nav_status;
    int sog;
    int rot;

    //Voyage
    String name;
    int ship_type;
    int draught;
    String dest;

    //Vessel
    public Record(int type, int id, double lat, double log, int nav_status, int sog, int rot, String timestamp, long offset) {
        this.type = type;
        this.id = id;
        this.lat = lat;
        this.log = log;
        this.nav_status = nav_status;
        this.sog = sog;
        this.rot = rot;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    //Base
    public Record(int type, int id, double lat, double log, String timestamp, long offset) {
        this.type = type;
        this.id = id;
        this.lat = lat;
        this.log = log;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    //Voyage
    public Record(int type, int id, String name, int ship_type, int draught, String dest, String timestamp, long offset) {
        this.type = type;
        this.id = id;
        this.timestamp = timestamp;
        this.name = name;
        this.ship_type = ship_type;
        this.draught = draught;
        this.dest = dest;
        this.offset = offset;
    }

    //General
    public Record(int type, int id, String timestamp) {
        this.type = type;
        this.id = id;
        this.timestamp = timestamp;
    }

    public String toDatabaseString() {
        //Double format
        DecimalFormat df = new DecimalFormat(".#######");
        df.setRoundingMode(RoundingMode.DOWN);

        //Split UNIX epoch from milliseconds
        //We still don't handle milliseconds, as the MonetDB insert doesn't have that precision
        String[] ts = timestamp.split("\\.");

        if (type < 4) {
            return "(" + id + "," + String.valueOf(df.format(lat)) + "," + String.valueOf(df.format(log)) + "," + nav_status + "," + sog + "," + rot + ",epoch(" + ts[0] + ")," + offset + ")";
        } else if (type == 4) {
            return "(" + id + "," + String.valueOf(df.format(lat)) + "," + String.valueOf(df.format(log)) + ",epoch(" + ts[0] + ")," + offset + ")";
        } else if (type == 5) {
            return "(" + id + ",'" + name + "'," + ship_type + "," + draught + ",'" + dest.replace("\\", " ") + "',epoch(" + ts[0] + ")," + offset + ")";
        } else {
            return "";
        }
    }

    public String toCopyInto() {
        Instant instantEvent = Instant.ofEpochSecond(Long.parseLong(timestamp.split("\\.")[0]));
        Date dateEvent = Date.from(instantEvent);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSSSSS");
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        DecimalFormat df = new DecimalFormat(".#######");
        df.setRoundingMode(RoundingMode.DOWN);

        if (type < 5) {
            String strLat = String.valueOf(df.format(lat)), strLog = String.valueOf(df.format(log));
            if (strLat.equals("")) {
                strLat = "0.0";
            } else if (strLog.equals("")) {
                strLog = "0.0";
            } else if (strLat.startsWith(".")) {
                strLat = "0" + strLat;
            } else if (strLog.startsWith(".")) {
                strLog = "0" + strLog;
            }

            //Vessel messages
            if (type < 4) {
                return id + "," + strLat + "," + strLog + "," + nav_status + "," + sog + "," + rot + "," + simpleDateFormat.format(dateEvent) + "," + offset;
            }
            //Base messages
            else {
                return id + "," + strLat + "," + strLog + "," + simpleDateFormat.format(dateEvent) + "," + offset;
            }
        }
        //Voyage messages
        else if (type == 5) {
            return id + ",\"" + name + "\"," + ship_type + "," + draught + ",\"" + dest + "\"," + simpleDateFormat.format(dateEvent) + "," + offset;
            //Other messages
        } else {
            //return id + "," + simpleDateFormat.format(dateEvent) + "\n";
            return "";
        }
    }
}