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

        public List<List<String>> parse(List<AisMessage> messages) {
            List<String> messagespos = new ArrayList<>();
            List<String> messages4 = new ArrayList<>();
            List<String> messages5 = new ArrayList<>();
            List<String> messages9 = new ArrayList<>();
            List<String> messages18 = new ArrayList<>();
            List<String> messages21 = new ArrayList<>();
            List<String> messages24 = new ArrayList<>();

            for(AisMessage msg : messages) {
                    switch(msg.getMsgId()) {
                        case 1:
                        case 2:
                        case 3:
                            messagespos.add(parseDecodedMessage(MessageType.One, msg, "foo"));
                            break;
                        case 4:
                            messages4.add(parseDecodedMessage(MessageType.Four, msg, "foo"));
                            break;
                        case 5:
                            messages5.add(parseDecodedMessage(MessageType.Five, msg, "foo"));
                            break;
                        case 9:
                            messages9.add(parseDecodedMessage(MessageType.Nine, msg, "foo"));
                            break;
                        case 18:
                            messages18.add(parseDecodedMessage(MessageType.Eighteen, msg, "foo"));
                            break;
                        case 21:
                            messages21.add(parseDecodedMessage(MessageType.TwentyOne, msg, "foo"));
                            break;
                        case 24:
                            messages24.add(parseDecodedMessage(MessageType.TwentyFour, msg, "foo"));
                            break;
                }
            }

            return List.of(
                messagespos,
                messages4,
                messages5,
                messages9,
                messages18,
                messages21,
                messages24
            );

        }

     private String parseDecodedMessage (MessageType type, AisMessage aisMessage, String timestamp) {
        switch(type) {
            case Eighteen: {
                    AisMessage18 decodedMessage = (AisMessage18) aisMessage;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                    }

                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                }
            case Five: {
                    AisMessage5 decodedMessage = (AisMessage5) aisMessage;
                    return String.format("%d,%s,%d,%s,%d,%s,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),decodedMessage.getName().replaceAll("[\\\\@',]", "").trim(),decodedMessage.getImo(),decodedMessage.getCallsign(),decodedMessage.getShipType(),decodedMessage.getDest().replaceAll("[\\\\@',]", "").trim(),decodedMessage.getDraught(),decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
                }
            case Four: {
                    AisMessage4 decodedMessage = (AisMessage4) aisMessage;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),timestamp);
                    }
                    return String.format("%d,%.2f,%.2f,%s",decodedMessage.getUserId(),181.0,91.0,timestamp);
                }
            case Nine: {
                    AisMessage9 decodedMessage = (AisMessage9) aisMessage;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getAltitude(), decodedMessage.getSog(), decodedMessage.getCog(),timestamp);
                    }

                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getAltitude(), decodedMessage.getSog(), decodedMessage.getCog(),timestamp);
                }
            case One: {
                    AisPositionMessage decodedMessage = (AisPositionMessage) aisMessage;
                    Position pos = decodedMessage.getValidPosition();
                    if (pos != null) {
                        return String.format("%d,%.2f,%.2f,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),pos.getLongitude(),pos.getLatitude(),decodedMessage.getNavStatus(),decodedMessage.getRot(), decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                    }
                    return String.format("%d,%.2f,%.2f,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),181.0,91.0,decodedMessage.getNavStatus(),decodedMessage.getRot(), decodedMessage.getSog(), decodedMessage.getCog(),decodedMessage.getTrueHeading(),timestamp);
                }
            case TwentyFour: {
                AisMessage24 decodedMessage = (AisMessage24) aisMessage;

                return String.format("%d,%s,%s,%d,%d,%d,%d,%d,%s",decodedMessage.getUserId(),
                decodedMessage.getName() != null ? decodedMessage.getName().replaceAll("[\\\\@',]", "").trim() :
                "", decodedMessage.getCallsign(),decodedMessage.getShipType(),decodedMessage.getDimBow(),decodedMessage.getDimStern(),decodedMessage.getDimPort(),decodedMessage.getDimStarboard(),timestamp);
            }
            case TwentyOne: {
                    AisMessage21 decodedMessage = (AisMessage21) aisMessage;
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