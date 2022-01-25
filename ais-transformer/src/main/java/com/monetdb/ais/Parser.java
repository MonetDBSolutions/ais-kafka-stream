package com.monetdb.ais;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessage18;
import dk.dma.ais.message.AisMessage21;
import dk.dma.ais.message.AisMessage24;
import dk.dma.ais.message.AisMessage4;
import dk.dma.ais.message.AisMessage5;
import dk.dma.ais.message.AisMessage9;
import dk.dma.ais.message.AisPositionMessage;
import dk.dma.enav.model.geometry.Position;

class Parser {
        enum MessageType {
            One,
            Four,
            Five,
            Nine,
            Eighteen,
            TwentyOne,
            TwentyFour,
        }

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

        public void writeToCsvFile(String fileNumber, List<AisMessage> messages) {
        List<Pair<MessageType, AisMessage>> messagespos = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages4 = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages5 = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages9 = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages18 = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages21 = new ArrayList<>();
        List<Pair<MessageType, AisMessage>> messages24 = new ArrayList<>();

        for(AisMessage msg : messages) {
                switch(msg.getMsgId()) {
                    case 1:
                    case 2:
                    case 3:
                        messagespos.add(new Pair<MessageType, AisMessage>(MessageType.One, msg));
                        break;
                    case 4:
                        messages4.add(new Pair<MessageType, AisMessage>(MessageType.Four, msg));
                        break;
                    case 5:
                        messages5.add(new Pair<MessageType, AisMessage>(MessageType.Five, msg));
                        break;
                    case 9:
                        messages9.add(new Pair<MessageType, AisMessage>(MessageType.Nine, msg));
                        break;
                    case 18:
                        messages18.add(new Pair<MessageType, AisMessage>(MessageType.Eighteen, msg));
                        break;
                    case 21:
                        messages21.add(new Pair<MessageType, AisMessage>(MessageType.TwentyOne, msg));
                        break;
                    case 24:
                        messages24.add(new Pair<MessageType, AisMessage>(MessageType.TwentyFour, msg));
                        break;
            }
        }
    }

    public void writeToFile(String header, List<Pair<MessageType,AisMessage>> messages, String fileName) {
        if(messages.size() <= 0) {
            return;
        }

        new Thread(() -> {
            FileWriter file = null;

            try {
                file = new FileWriter(fileName);

                file.append(header + "\n");

                for(Pair<MessageType, AisMessage> msg : messages) {
                    String out = parseDecodedMessage(msg, "test");
                    file.append(out + "\n");
                }
            }catch(Exception e)  {
                System.out.println(e.getLocalizedMessage());
            }

            try {
                if(file != null) {
                    file.flush();
                    file.close();
                }

            } catch(Exception s) {
                    System.out.println(s.getLocalizedMessage());
            }
        }).start();
    }
    
     private String parseDecodedMessage (Pair<MessageType, AisMessage> aisMessage, String timestamp) {
        switch(aisMessage.left) {
            case Eighteen: {
                    AisMessage18 decodedMessage = (AisMessage18) aisMessage.right;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                    }

                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                }
            case Five: {
                    AisMessage5 decodedMessage = (AisMessage5) aisMessage.right;
                    return String.format("%d,%s,%d,%s,%d,%s,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),decodedMessage.getName().replaceAll("[\\\\@',]", "").trim(),decodedMessage.getImo(),decodedMessage.getCallsign(),decodedMessage.getShipType(),decodedMessage.getDest().replaceAll("[\\\\@',]", "").trim(),decodedMessage.getDraught(),decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
                }
            case Four: {
                    AisMessage4 decodedMessage = (AisMessage4) aisMessage.right;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),timestamp);
                    }
                    return String.format("%d,%.2f,%.2f,%s",decodedMessage.getUserId(),181.0,91.0,timestamp);
                }
            case Nine: {
                    AisMessage9 decodedMessage = (AisMessage9) aisMessage.right;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getAltitude(), decodedMessage.getSog(), decodedMessage.getCog(),timestamp);
                    }

                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getAltitude(), decodedMessage.getSog(), decodedMessage.getCog(),timestamp);
                }
            case One: {
                    AisPositionMessage decodedMessage = (AisPositionMessage) aisMessage.right;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getNavStatus(),decodedMessage.getRot(), decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                    }
                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getNavStatus(),decodedMessage.getRot(), decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                }
            case TwentyFour: {
                AisMessage24 decodedMessage = (AisMessage24) aisMessage.right;

                return String.format("%d,%s,%s,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),
                decodedMessage.getName() != null ? decodedMessage.getName().replaceAll("[\\\\@',]", "").trim() :
                "", decodedMessage.getCallsign(),decodedMessage.getShipType(),decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
            }
            case TwentyOne: {
                    AisMessage21 decodedMessage = (AisMessage21) aisMessage.right;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%d,%s,%.2f,%.2f,%d,%d,%d,%d,%s",decodedMessage.getUserId(),decodedMessage.getAtonType(),decodedMessage.getName().replaceAll("[\\\\@',]", "").trim(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
                    }

                    return String.format("%d,%d,%s,%.2f,%.2f,%d,%d,%d,%d,%s",decodedMessage.getUserId(),decodedMessage.getAtonType(),decodedMessage.getName().replaceAll("[\\\\@',]", "").trim(),181.0,91.0,decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
                }
            default:
                return null;
        }
    }

    public void printSummary(List<AisMessage> decoded){
        Map<Object, Long> out = decoded.stream().collect(Collectors.groupingBy(msg -> msg.getClass(), Collectors.counting()));

        System.out.println(out);
    }
}