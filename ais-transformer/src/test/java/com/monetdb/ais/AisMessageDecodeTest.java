package com.monetdb.ais;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.io.Files;
import org.junit.Test;

import dk.dma.ais.message.AisMessage;

public class AisMessageDecodeTest
{
    @Test
    public void successfullyDecodeAMessage()
    {
        String msg = "!AIVDM,1,1,,B,140Uu38000PFMw6Mv:b9p`F8P>`<,0*17";

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertEquals( messages.size(), 1 );
        assertEquals( messages.get(0).getMsgId(), 1 );
    }

    @Test
    public void successfullyDecodeAMessageWithOtherPrefix()
    {
        String msg = "$AIVDM,1,1,,B,140Uu38000PFMw6Mv:b9p`F8P>`<,0*17";

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertEquals( messages.size(), 1 );
        assertEquals( messages.get(0).getMsgId(), 1 );
    }

    @Test
    public void successfullyDecodeAMessageWithPrefix()
    {
        String msg = "1471629655.294269 !AIVDM,1,1,,A,13aN2h0P00P@ojTMLSkMU?wl2D2T9HW>`<,0*21";

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertEquals( messages.size(), 1 );
        assertEquals( messages.get(0).getMsgId(), 1 );
    }

    @Test
    public void successfullyDecodeMultipleMessages()
    {
        String msg = "!AIVDM,1,1,,B,140Uu38000PFMw6Mv:b9p`F8P>`<,0*17"
                     .concat("\n")
                     .concat("!AIVDM,1,1,,A,33aL>IhP0KPFJ;`MugtPQ?v82>`<,0*33")
                     .concat("\n")
                     .concat("!AIVDM,1,1,,B,23aDoj0P1HPF`7RMvBQCVOv82>`<,0*62");

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertTrue( messages.size() == 3 );
    }

    @Test
    public void successfullyDecodeMultipleMessagesWithSameId()
    {

        String msg = "!AIVDM,2,1,0,B,53aGFVl000010K;?S60D8DrlP4E`E:222222221?9H;5540Ht051DSQE,0*02\n"
                     .concat("!AIVDM,2,2,0,B,FQC`88888888880,2*13\n")
                     .concat("!AIVDM,2,1,0,A,5815=k@2AG=EKMaWB21A84@F1HU<Ttr222222216H`P@@68l0JDSm51DQ0CH,0*3B\n")
                     .concat("!AIVDM,2,2,0,A,88888888880,2*24\n")
                     .concat("!AIVDM,2,1,0,A,56:C:gP2D:Q88e=CJ21PTr19Dr3>22222222220l1905<61i0=FR3mTj,0*12\n")
                     .concat("!AIVDM,2,2,0,A,0C`000000000000,2*07\n")
                     .concat("!AIVDM,2,1,0,A,53v0BF42<s3m<`h:221=@Dp61<<4p@Tp5HT<621533K:D6:7?>BjAC;Akm1@,0*2A\n")
                     .concat("!AIVDM,2,2,0,A,SlQsBjAC880,2*13\n")
                     .concat("!AIVDM,2,1,0,A,58IAkJ42CO3SUK;;;R1@P4h5=<60PDhh5>222217AsQAR6:f0K3S4Q3H8888,0*65\n")
                     .concat("!AIVDM,2,2,0,A,88888888880,2*24\n")
                     .concat("!AIVDM,2,1,0,B,53=6>802A@47TP7;K;H4hhDM862222222222221?BHQI56<60>Si1DlhH888,0*3E\n")
                     .concat("!AIVDM,2,2,0,B,88888888880,2*27\n")
                     .concat("!AIVDM,2,1,0,A,58JC3<02?JUkUKOWS:1HTdTpN0t@E=<622222216;0l=965;0>31H20E,0*32\n")
                     .concat("!AIVDM,2,2,0,A,TQH888888888880,2*51\n")
                     .concat("!AIVDM,2,1,0,A,58JC3<02?JUkUKOWS:1HTdTpN0t@E=<622222216;0l=965;0>31H20ETQH8,0*47\n")
                     .concat("!AIVDM,2,2,0,A,88888888880,2*24\n")
                     .concat("!AIVDM,2,1,0,B,53`oT802<cj90D<p000I8ET40000000000000017<h@8:69i0ATm1E52CP00,0*7F\n")
                     .concat("!AIVDM,2,2,0,B,00000000000,2*27\n")
                     .concat("!AIVDM,3,1,0,A,802UMs@0D001Hh@8P2F2d@ltCCDw0<00001o`2FqHPmE:@0w0<00001n,0*68\n")
                     .concat("!AIVDM,3,2,0,A,d2F4whlw5`0w00010Wkd`2FL:0m=e`Mw1@010WkM@2FAj0m9RSSw2@01,0*6D\n")
                     .concat("!AIVDM,3,3,0,A,0W9DL2F9e@m1eLgw00,4*49\n")
                     .concat("!AIVDM,2,1,0,B,53ps3F400001=0O?7KT4p<Thh61Lh6oKR222220N0h@3340Ht6Ek0A6Dk0H8,0*25\n")
                     .concat("!AIVDM,2,2,0,B,88888888880,2*27\n")
                     .concat("!AIVDM,2,1,0,B,802R5Ph0GhFJ<i`TdVLLP89U<b06EuOwgqu9wnSwe7wvlOwwsAwwnSGm,0*55\n")
                     .concat("!AIVDM,2,2,0,B,wvwt,0*17\n")
                     .concat("!AIVDM,2,1,0,B,55SpW8029VAk<EaWV20p4LP4n222222222222200000006<d0G4iE5@PC888,0*59\n")
                     .concat("!AIVDM,2,2,0,B,88888888880,2*27\n")
                     .concat("!AIVDM,2,1,0,A,53P<tRH000038I0W@01<tU8DD00000000000000T00000400050000000000,0*05\n")
                     .concat("!AIVDM,2,2,0,A,00000000000,2*24\n")
                     .concat("!AIVDM,2,1,0,B,55N`tAT00001L@SWSO0mDU:0lTh4<R222222220N1h>,0*29\n")
                     .concat("!AIVDM,2,2,0,B,4451<0hARDj2CQp8888888888880,2*2A");

        List<AisMessage> messages = Decoder.decode(msg) ;

        System.out.println(messages.size());
        assertTrue( messages.size() == 15 );
    }

    @Test
    public void successfullyDecodeMessagesWithHighId()
    {
        String msg = "!AIVDM,1,1,,B,83aI8shj2d<d<=u<LPI8G@O000t0,0*58"
                     .concat("\n")
                     .concat("!AIVDM,2,1,8,B,802R5Ph0GhDs2QbmsNLLPpR<AR06EuOwgwl?wnSwe7wvlOwwsAwwnSGm,0*7A")
                     .concat("\n")
                     .concat("!AIVDM,2,2,8,B,wvwt,0*1F")
                     .concat("\n")
                     .concat("!AIVDM,2,1,9,B,56:E3wT2E:Ml9Th6220@tpN0H4pN0QD60<PDr39J1pj<15S:N8m2@C`2,0*5F")
                     .concat("\n")
                     .concat("!AIVDM,2,2,9,B,RC`888888888880,2*67");
                    
 

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertTrue( messages.size() == 3 );
    }

    @Test
    public void successfullyDecodeMessagesInWrongOrder()
    {
        String msg = "!AIVDM,1,1,,B,83aI8shj2d<d<=u<LPI8G@O000t0,0*58"
                     .concat("\n")
                     .concat("!AIVDM,2,1,8,B,802R5Ph0GhDs2QbmsNLLPpR<AR06EuOwgwl?wnSwe7wvlOwwsAwwnSGm,0*7A")
                     .concat("\n")
                     .concat("!AIVDM,2,2,8,B,wvwt,0*1F")
                     .concat("\n")
                     .concat("!AIVDM,2,1,9,B,56:E3wT2E:Ml9Th6220@tpN0H4pN0QD60<PDr39J1pj<15S:N8m2@C`2,0*5F")
                     .concat("\n")
                     .concat("!AIVDM,2,2,9,B,RC`888888888880,2*67");
                    
 

        List<AisMessage> messages = Decoder.decode(msg) ;

        assertTrue( messages.size() == 3 );
    }

    @Test
    public void decodeFromFile()
    {
        String content = readFile("AIS_test.txt");

        List<AisMessage> messages = Decoder.decode(content) ;

        assertTrue( messages.size() == 23 );
    }

    @Test
    
    public void decodeMultipleMessages()
    {
        String content = readFile("AIS_test_multiple_messages.txt");

        List<AisMessage> messages = Decoder.decode(content) ;

        assertTrue( messages.size() == 24 );
    }

    @Test
    
    public void decodeMultipleMessagesNotSequential()
    {
        String content = readFile("AIS_test_multiple_messages_not_sequential.txt");

        List<AisMessage> messages = Decoder.decode(content) ;

        assertTrue( messages.size() == 5 );
    }

    @Test
    public void decodeMissingMessages()
    {
        String content = readFile("AIS_test_multiple_messages_some_missing.txt");

        List<AisMessage> messages = Decoder.decode(content) ;

        assertTrue( messages.size() == 4 );
    }


    public String readFile(String path) {
        List<String> content = new ArrayList<>();
        try {
            String currentPath = System.getProperty("user.dir") + "/src/test/java/com/monetdb/ais/" + path;
            content = Files.readLines(new File(currentPath.toString()), Charset.defaultCharset());
        }catch(IOException e) {
            System.out.println(e.getMessage());
        }

        String result = content.stream()
            .map(n -> String.valueOf(n)) 
            .collect(Collectors.joining("\n"));

        return result;
    } 
}
