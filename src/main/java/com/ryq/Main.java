package com.ryq;

import com.ryq.Kafka.Consumer;

import java.util.Arrays;
import java.util.List;

public class Main {
     public static void main(String[] args) {
         // topicnames
         List<String> areasOfInterest = Arrays.asList(
                 "test2"     // 测试topic
         );
         Consumer consumer = new Consumer(areasOfInterest);
         Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));
         consumer.run();
     }
}
