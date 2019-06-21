import Runnables.ConsumerThread;
import Runnables.ProducerContainer;
import Serializer.JsonEncoder;
import com.beust.jcommander.JCommander;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import constants.Args;
import model.KafkaMessage;
import model.Type;

import java.io.File;
import java.util.Locale;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;



/*
   Typical message to send, remember to increase the MessageID
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello Soup","topic":["T4"]}
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2"]}
  {"messageID":123456,"clientID":2,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2","T3"]}

*/


// Command to run jar
// java -jar 0.jar -Topic T1 -TransactionID 1 -GroupID 1 -ID 1
// java -jar 0.jar -Topic T2 -TransactionID 2 -GroupID 2 -ID 2

// Notes:
// Every "AtomicNode" is a detached process, what this mean is that they do not know of any other topic or any other
// AtomicNodes, A node might be the only one, or there might be several others. Since this is a detached content based
// atomic multicast system everything should be detached until needed.
// until a client tells them about it, after they have agreed on the message the
// "AtomicNode" forgets about that topic, as the topic-relation only lives inside the message.

// I think we need three stages for agreeing on total order.
// 1. Receive from client, notify others
// 2. Receive acks, decide on an ACK message.
// 3. Receive Decided, when all decided messages #ofTopics is received, send delivery.

public class App {

    public static final int NUMBER_OF_MESSAGES_TO_SEND = 20000;


    public static void main(String[] arguments) {
        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(arguments);

        JsonEncoder json = new JsonEncoder();
        System.out.println("Press enter to start!");
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        reader.next();

        MetricRegistry metricRegistry = new MetricRegistry();
        final Meter incoming = metricRegistry.meter("incoming");
        final Meter outgoing = metricRegistry.meter("outgoing");

        File file = new File("/home/stud/frtvedt/project/data/eval/" + args.Topic + "/");
        final CsvReporter reportercsv = CsvReporter.forRegistry(metricRegistry)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(file);
        reportercsv.start(15, TimeUnit.SECONDS);

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(15, TimeUnit.SECONDS);

        // Create ProducerContainer singleton
        final ProducerContainer producer = new ProducerContainer(args.ID, args.TransactionID, outgoing);

        final String[] topics = new String[]{"T1", "T2", "T3", "T4", "T5"};

        final String[][] sendTo = new String[][]{
                new String[]{"T1", "T2", "T3", "T4"}, // 0

                new String[]{"T1",}, // 1
                new String[]{"T2",}, // 2
                new String[]{"T3"}, // 3
                new String[]{"T4"}, // 4

                new String[]{"T1", "T2"}, // 5
                new String[]{"T1", "T3"}, // 6
                new String[]{"T1", "T4"}, // 7
                new String[]{"T2", "T3"}, // 8
                new String[]{"T2", "T4"}, // 9
                new String[]{"T3", "T4"}, // 10

                new String[]{"T1", "T2", "T3"}, // 11
                new String[]{"T1", "T3", "T4"}, // 12
                new String[]{"T2", "T3", "T4"}, // 13
                new String[]{"T2", "T4", "T1"}, // 14
                new String[]{"T5"}, // 15


                new String[]{"T1", "T2", "T3", "T4", "T5"}, // 16
                new String[]{"T1", "T3", "T4", "T5"}, // 17
                new String[]{"T2", "T3", "T4", "T5"}, // 18
                new String[]{"T2", "T4", "T1", "T5"}, // 19


        };


        Random rand = new Random();
        // Start Evaluation. (Baseline)
        Thread thread = null;
        if (args.Topic.equalsIgnoreCase("t1")) {
            thread = createThread333333(producer, sendTo, 1);
        } else if (args.Topic.equalsIgnoreCase("t2")) {
            thread = createThread333333(producer, sendTo, 2);
        } else if (args.Topic.equalsIgnoreCase("t3")) {
            thread = createThread333333(producer, sendTo, 3);
        } else if (args.Topic.equalsIgnoreCase("t4")) {
            thread = createThread333333(producer, sendTo, 4);
        } else if (args.Topic.equalsIgnoreCase("t5")) {
            thread = createThread333333(producer, sendTo, 15);
        } else {
            System.exit(123);
        }


        // Create a consumer that listens to incoming messages on a topic.
        ConsumerThread cThread = new ConsumerThread(producer, args.Topic, args.GroupID, args.ID, NUMBER_OF_MESSAGES_TO_SEND, incoming);
        Thread t = new Thread(cThread);
        t.setName(args.Topic);
        long startTime = System.currentTimeMillis();
        thread.start();
        t.start();


        // Do we need a consensus thread???
        // ProducerContainer can be a singelton, just use java synchronized before sending something, if a node
        // has more than one topic to be responsible for.

        // Use java.Exchanger to exchange data between threads. (Not sure if needed for this)

        while (true) {
            System.out.println("quit: 0");
            int stop = reader.nextInt();
            if (stop == 0) {
                System.out.println("Total execution time: " + (startTime - cThread.getEndTime()) + "ms");
            }
        }
    }


    public static Thread createThreadSingleTopic(ProducerContainer producer, String[][] sendTo, int index) {
        long startTime = System.currentTimeMillis();
        Random rand = new Random();
        Thread thread = new Thread(() -> {
            int counterSingleTopic = 0;
            int counterTwoTopics = 0;
            int counterThreeTopics = 0;
            for (int i = 0; i < NUMBER_OF_MESSAGES_TO_SEND; ++i) {

                int x = rand.nextInt(100);
                int indexSender = 3;
                int mid = i + (indexSender + 50000);
                producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[indexSender]), sendTo[indexSender]);
                counterSingleTopic++;

            }
            long endTime = System.currentTimeMillis();
            System.out.println("Spent " + (endTime - startTime) + "ms to send  " + (counterSingleTopic + counterTwoTopics + counterThreeTopics) + " topics");
        });
        return thread;
    }


    public static Thread createThreadTenTenNinety(ProducerContainer producer, String[][] sendTo, int index) {
        long startTime = System.currentTimeMillis();
        Random rand = new Random();
        Thread thread = new Thread(() -> {
            int counterSingleTopic = 0;
            int counterTwoTopics = 0;
            int counterThreeTopics = 0;
            for (int i = 0; i < NUMBER_OF_MESSAGES_TO_SEND; ++i) {


                int x = rand.nextInt(100);
                int indexSender = 15;

                int mid = i + (indexSender * 1000000);
                if (x <= 15) { // 10%
                    producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[16]), sendTo[indexSender]); // REMEMBER THIS ONLY SENDS TO TWO
                    counterTwoTopics++;
                } else if (x < 100) {
                    producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[indexSender]), sendTo[indexSender]);
                    counterSingleTopic++;
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total counterSingleTopic: " + counterSingleTopic);
            System.out.println("Total counterTwoTopics: " + counterTwoTopics);
            System.out.println("Total counterThreeTopics: " + counterThreeTopics);
            System.out.println("Spent " + (endTime - startTime) + "ms to send  " + (counterSingleTopic + counterTwoTopics + counterThreeTopics) + " topics");
        });
        return thread;
    }


    public static Thread createThread151580(ProducerContainer producer, String[][] sendTo, int index) {
        long startTime = System.currentTimeMillis();
        Random rand = new Random();
        Thread thread = new Thread(() -> {
            int counterSingleTopic = 0;
            int counterTwoTopics = 0;
            int counterThreeTopics = 0;
            for (int i = 0; i < NUMBER_OF_MESSAGES_TO_SEND; ++i) {


                int x = rand.nextInt(100);
                int indexSender = 1;

                int mid = i + (indexSender * 10000);
                if (x <= 20) { // 10%
                    producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[16]), sendTo[indexSender]); // REMEMBER THIS ONLY SENDS TO TWO
                    counterTwoTopics++;
                } else if (x < 100) {
                    producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[indexSender]), sendTo[indexSender]);
                    counterSingleTopic++;
                }
                //"messageID":479

            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total counterSingleTopic: " + counterSingleTopic);
            System.out.println("Total counterTwoTopics: " + counterTwoTopics);
            System.out.println("Total counterThreeTopics: " + counterThreeTopics);
            System.out.println("Spent " + (endTime - startTime) + "ms to send  " + (counterSingleTopic + counterTwoTopics + counterThreeTopics) + " topics");
        });
        return thread;
    }


    public static Thread createThread333333(ProducerContainer producer, String[][] sendTo, int index) {
        long startTime = System.currentTimeMillis();
        Random rand = new Random();
        Thread thread = new Thread(() -> {
            int counterSingleTopic = 0;
            int counterTwoTopics = 0;
            int counterThreeTopics = 0;
            for (int i = 0; i < NUMBER_OF_MESSAGES_TO_SEND; ++i) {


                int x = rand.nextInt(100);
                int indexSender = 4;

                int mid = i + (indexSender);
                producer.sendMessage(new KafkaMessage(mid, indexSender, Type.ClientMessage, "Hello", sendTo[indexSender]), sendTo[indexSender]);
                counterSingleTopic++;


                counterSingleTopic++;

            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total counterSingleTopic: " + counterSingleTopic);
            System.out.println("Total counterTwoTopics: " + counterTwoTopics);
            System.out.println("Total counterThreeTopics: " + counterThreeTopics);
            System.out.println("Spent " + (endTime - startTime) + "ms to send  " + (counterSingleTopic + counterTwoTopics + counterThreeTopics) + " topics");
        });
        return thread;
    }
}