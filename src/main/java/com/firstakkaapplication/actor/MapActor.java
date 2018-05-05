package com.firstakkaapplication.actor;

import akka.actor.UntypedActor;
import com.firstAkkaApplication.messages.MapData;
import com.firstAkkaApplication.messages.WordCount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MapActor extends UntypedActor {
    
    String[] STOP_WORDS = { "a", "am", "an", "and", "are", "as", "at",
            "be","do", "go", "if", "in", "is", "it", "of", "on", "the", "to" };
    List<String> STOP_WORDS_LIST = Arrays.asList(STOP_WORDS);
    
    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            String work = (String) message;
            // map the words in the sentence and send the result
            // to MasterActor
            getSender().tell(evaluateExpression(work));
        } else
            unhandled(message);
    }
    private MapData evaluateExpression(String line) {
        List<WordCount> dataList = new ArrayList<WordCount>();
        StringTokenizer parser = new StringTokenizer(line);
        while (parser.hasMoreTokens()) {
            String word = parser.nextToken().toLowerCase();
            if (!STOP_WORDS_LIST.contains(word)) {
                dataList.add(new WordCount(word,Integer.valueOf(1)));
            }
        }
        return new MapData(dataList);
    }
}