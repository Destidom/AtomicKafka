import Runnables.ConsumerThread;
import Runnables.ProducerContainer;
import Serializer.JsonEncoder;
import com.beust.jcommander.JCommander;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import consensus.AtomicMulticast;
import constants.Args;
import constants.Constants;
import model.KafkaMessage;

import java.io.File;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/*
   Typical message to send, remember to increase the MessageID
  {"messageID":124,"clientID":1,"messageType":"ClientMessage","value":"Hello Soup","topic":["T1", "T2"]}
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello Soup","topic":["T1","T2","T3","T4"]}
  {"messageID":3,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2","T3"]}
  {"messageID":4000,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T3","T2"]}



  {"messageID":10000,"senderID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1"],"offset":-999,"timeStamp":-999}
  {"messageID":7,"senderID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1"]}
  {"messageID":2893,"senderID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1"],"offset":-999,"timeStamp":-999}

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
        final Meter decided = metricRegistry.meter("decided");
        File file = new File("/home/stud/frtvedt/project/data/" + args.Topic + "/");

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
        reporter.start(20, TimeUnit.SECONDS);

        // Create ProducerContainer singleton
        System.out.println("b " + args.kafka_brokers);
        final ProducerContainer producer = new ProducerContainer(args.ID, args.TransactionID + args.Topic, outgoing, args.kafka_brokers);
        // Create a consumer that listens to incoming messages on a topic.
        ConsumerThread cThread = new ConsumerThread(producer, args.Topic, args.GroupID, args.ID, incoming, decided, args.kafka_brokers);
        Thread t = new Thread(cThread);
        t.setName(args.Topic);
        t.start();

        /*Runnable test = new Runnable() {
            @Override
            public void run() {
                AtomicMulticast consensus = AtomicMulticast.getInstance();
                try {
                    while(true) {
                        System.out.println("Size of DeliveryHeap " + consensus.getDeliveryHeap().size());
                        System.out.println("Peak of DeliveryHeap " + consensus.getDeliveryHeap().peek());

                        if( consensus.getMap() != null && consensus.getDeliveryHeap().peek() != null) {
                            ConcurrentHashMap<Integer, KafkaMessage> state = consensus.getMap().get(consensus.getDeliveryHeap().peek().getMessageID());
                            if (state != null && state.size() > 0)
                                System.out.println("Peak of DeliveryHeap " + state.toString());
                        }
                        Thread.sleep(5000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        };

        Thread ta = new Thread(test);
        ta.start();*/
        // Do we need a consensus thread???
        // ProducerContainer can be a singelton, just use java synchronized before sending something, if a node
        // has more than one topic to be responsible for.

        // Use java.Exchanger to exchange data between threads. (Not sure if needed for this)
        while (true) {
            System.out.println("quit: 0");
            int stop = reader.nextInt();
            if (stop == 0) {
                cThread.stop();
                break;
            } else {
                Meter meter = incoming;
                System.out.println("Mean rate: " + meter.getMeanRate());
                System.out.println("1-min rate: " + meter.getOneMinuteRate());
                System.out.println("5-min rate: " + meter.getFiveMinuteRate());
                System.out.println("15-min rate: " + meter.getFifteenMinuteRate());
                System.out.println("Messages: " + meter.getCount());


            }
        }
    }
}
