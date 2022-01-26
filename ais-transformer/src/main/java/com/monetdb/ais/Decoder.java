package com.monetdb.ais;

import dk.dma.ais.message.*;
import dk.dma.ais.sentence.Vdm;
import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.sentence.SentenceException;

import java.util.*;
import java.util.stream.Collectors;
import java.text.SimpleDateFormat;

//AIS message decoder
// TODO: add logging
class Decoder {

    /**
     * Decodes the message
     * The sentence layer is in the following format:
     * !AIVDM,2,1,,B,13aJIHhP00PFSGlMvFmsdgv`2H;N,0*22
     * 
     * @param codedMessage: the message string
     * @param vdm
     * @return A list of AisMessages
     */
    public static List<AisMessage> decode(String codedMessage) {
        List<AisMessage> messages = new ArrayList<>();
        List<Tuple> toBeDecoded = new ArrayList<>();

        String[] splitted = codedMessage.strip().split("\n");

        for(String _split : splitted) {
            Vdm vdm = new Vdm();

            // Some messages can be in a different form:
            // 1471629655.294269 !AIVDM,1,1,,A,13aN2h0P00P@ojTMLSkMU?wl2D2T9HW>`<,0*21
            Character prefix = _split.toCharArray()[0];
            if(prefix == '$') {
                prefix = '!';
            }
            if( prefix != '!') {
                String[] arr = _split.split(" ");
                _split = arr[1];
            }

            String[] splitMsg = _split.split(",");
            int fragments = Integer.parseInt(splitMsg[1]);
            int fragmentNumber = Integer.parseInt(splitMsg[2]);
            int fragmentId = -1;

            if(fragments > 1 && !splitMsg[3].isEmpty()) {
                fragmentId  = Integer.parseInt(splitMsg[3]);
                toBeDecoded.add(new Tuple(fragmentId, fragmentNumber, fragments, _split));
                continue;
            }

            if(fragments == fragmentNumber) {
                AisMessage tmp = decodeMessage(_split, vdm);

                if(tmp != null) {
                    messages.add(tmp);
                }
            }
            
        }

        if(!toBeDecoded.isEmpty()) {
            List<AisMessage> pair = parseLeftovers(toBeDecoded);
            List<AisMessage> leftovers = pair;

            if(leftovers.size() > 0) {
                messages.addAll(leftovers);
            }
        }

        return messages;
    }

    public static List<AisMessage> decodeSingleMessage(List<String> splitted) {
        List<AisMessage> messages = new ArrayList<>();
        List<Tuple> toBeDecoded = new ArrayList<>();

        for(String _split : splitted) {
            Vdm vdm = new Vdm();

            // Some messages can be in a different form:
            // 1471629655.294269 !AIVDM,1,1,,A,13aN2h0P00P@ojTMLSkMU?wl2D2T9HW>`<,0*21
            Character prefix = _split.toCharArray()[0];
            if(prefix == '$') {
                prefix = '!';
            }
            if( prefix != '!') {
                String[] arr = _split.split(" ");
                _split = arr[1];
            }

            String[] splitMsg = _split.split(",");
            int fragments = Integer.parseInt(splitMsg[1]);
            int fragmentNumber = Integer.parseInt(splitMsg[2]);
            int fragmentId = -1;

            if(fragments > 1 && !splitMsg[3].isEmpty()) {
                fragmentId  = Integer.parseInt(splitMsg[3]);
                toBeDecoded.add(new Tuple(fragmentId, fragmentNumber, fragments, _split));
                continue;
            }

            if(fragments == fragmentNumber) {
                AisMessage tmp = decodeMessage(_split, vdm);

                if(tmp != null) {
                    messages.add(tmp);
                }
            }
            
        }

        if(!toBeDecoded.isEmpty()) {
            List<AisMessage> pair = parseLeftovers(toBeDecoded);
            List<AisMessage> leftovers = pair;

            if(leftovers.size() > 0) {
                messages.addAll(leftovers);
            }
        }

        return messages;
    }


    /**
     * Parse the leftovers into a list of AisMessages
     * @param leftovers (A list of Tuple's)
     * @return List<AisMessage>
     */
    public static List<AisMessage> parseLeftovers(List<Tuple> leftovers) {
        List<AisMessage> messages = new ArrayList<>();
        int failed = 0;

        if(leftovers.size() > 1) {
            leftovers = parsePartialMessagesIntoMessages(leftovers);
        }

        for(Tuple t : leftovers) {
                try {
                    Vdm _vdm = t.parse();

                    if(_vdm != null) {
                        AisMessage msg = AisMessage.getInstance(_vdm);
                        messages.add(msg);
                    }
                } catch (AisMessageException | SixbitException e) {
                    String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(Calendar.getInstance().getTime());

                    String exceptionString = "Exception thrown: ".concat(e.getLocalizedMessage())
                                                                 .concat("\n")
                                                                 .concat("With exception type: " + e.getClass().toString())
                                                                 .concat("\n")
                                                                 .concat("Message: " + t.right.toString()) 
                                                                 .concat("\n")
                                                                 .concat("Message length: " + t.right.size())
                                                                 .concat("\n")
                                                                 .concat("at: " + timeStamp)
                                                                 .concat("\n");
                     
                    System.out.println(exceptionString);
                    failed += 1;
                    continue;
                }
            }

        System.out.println("amount of failed " + failed);

        return messages;
    }

    private static List<Tuple> parsePartialMessagesIntoMessages(List<Tuple> messages) {
        List<Tuple> parsed = new ArrayList<Tuple>();
        int newIdx = 0;

        for(int i = 0; i < messages.size(); i++) {
           int fragAmount = messages.get(i).amountOfFragments; 
           int fragmentId = messages.get(i).id;
           int end  = newIdx + fragAmount;
           List<String> temp = new ArrayList<>();

           for(int x = newIdx; x < end; x++) {
               Tuple currentMsg = messages.get(x);
               if(fragmentId == currentMsg.id) {
                 temp.add(String.join(" ", currentMsg.right));
               }
               else {
                   newIdx = x;
                   break;
               }
           }

           if(temp.size() == 0) {
               continue;
           }

           Tuple tuple = new Tuple(temp);
           parsed.add(tuple);

           newIdx = end;

           if(newIdx >= messages.size() || end > messages.size()) {
               break;
           }

           i = newIdx;
        }

        return parsed;
    }

    public static AisMessage decodeMessage(String codedMessage, Vdm vdm) {
        AisMessage aisMessage = null;

        try {
            Vdm tempVdm = new Vdm();
            tempVdm.parse(codedMessage);
            aisMessage = AisMessage.getInstance(tempVdm);
        } catch (SentenceException | AisMessageException | SixbitException e) {
            String exceptionString = "Exception thrown (decodeMessage): ".concat(e.getLocalizedMessage())
                                                                         .concat("\n")
                                                                         .concat("Payload: " + codedMessage)
                                                                         .concat("\n");

            System.out.println(exceptionString);
        }

        return aisMessage;
    }


    public static Vdm parseVdm(Vdm vdm, String msg) {
        try {
            vdm.parse(msg);
        }catch(SentenceException e ) {
            System.err.println(e.getLocalizedMessage());
        }

        return vdm;
    }
}


class Tuple {
    public int id;
    public int currentFragmentNumber;
    public int amountOfFragments;
    public List<String> right;

    public Tuple(int id, int currentFragmentNumber, int amountOfFragments,
                String right) {
        this.right = new ArrayList<>();
        this.id = id;
        this.currentFragmentNumber = currentFragmentNumber;
        this.amountOfFragments = amountOfFragments;

        this.right.add(right);
    }

    public Tuple(int id, int currentFragmentNumber, int amountOfFragments,
                List<String> right) {
        this.right = new ArrayList<>();
        this.id = id;
        this.currentFragmentNumber = currentFragmentNumber;
        this.amountOfFragments = amountOfFragments;

        this.right.addAll(right);
    }

    public Tuple(List<String> right) {
        this.right = new ArrayList<>();
        this.id = -1;
        this.currentFragmentNumber = -1;
        this.amountOfFragments = -1;

        this.right.addAll(right);
    }

    public void add(String _input) {
        this.right.add(_input);
    }

    public Vdm parse() {
        Vdm _vdm = new Vdm();
        for(String in : this.sort(this.right)) {
           try {
                _vdm.parse(in);
           }catch(SentenceException e) {
               String exceptionString = "Tuple Parse: " + e.getLocalizedMessage()
                                        .concat("\n")
                                        .concat("message:")
                                        .concat(String.join(" ", this.right))
                                        .concat("\n");

               System.out.println(exceptionString);

               return null;
           }
        }

        return _vdm;
    }

    private List<String> sort(List<String> input) {
        List<Pair<Integer, String>> shuffle = new ArrayList<>();
        List<String> output = new ArrayList<>();

        for(String i : input) {
            String[] split = i.split(",");

            int fragmentNumber = Integer.parseInt(split[2]);

            Pair<Integer, String> pair = new Pair<Integer, String>(fragmentNumber, i);

            shuffle.add(pair);
        }
        
        shuffle = shuffle.stream().sorted(Comparator.comparingInt(x -> x.getLeft())).collect(Collectors.toList());

        for(Pair<Integer, String> s : shuffle) {
            output.add((String)s.right);
        }

        return output;
    }

}
