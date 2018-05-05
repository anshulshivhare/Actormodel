package com.firstakkaapplication.actor;

import akka.actor.UntypedActor;
import com.firstAkkaApplication.messages.MapData;
import com.firstAkkaApplication.messages.ReduceData;
import com.firstAkkaApplication.messages.WordCount;

import java.util.HashMap;
import java.util.List;

public class ReduceActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof MapData) {
            MapData mapData = (MapData) message;
            // reduce the incoming data and forward the result to
            // Master actor
    
            getSender().tell(reduce(mapData.getDataList()));
        } else
            unhandled(message);
    }
    private ReduceData reduce(List<WordCount> dataList) {
        HashMap<String, Integer> reducedMap = new HashMap<String,
                Integer>();
        for (WordCount wordCount : dataList) {
            if (reducedMap.containsKey(wordCount.getWord())) {
                Integer value = (Integer)
                        reducedMap.get(wordCount.getWord());
                value++;
                reducedMap.put(wordCount.getWord(), value);
            } else {
                reducedMap.put(wordCount.getWord(),
                        Integer.valueOf(1));
            }
        }
        return new ReduceData(reducedMap);
    }


}
