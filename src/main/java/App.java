import Runnables.ConsumerThread;
import Runnables.ProducerContainer;
import Serializer.JsonEncoder;
import com.beust.jcommander.JCommander;
import constants.Args;
import model.KafkaMessage;

import java.util.PriorityQueue;
import java.util.Scanner;

/*
   Typical message to send, remember to increase the MessageID
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello Soup","topic":["T1"]}
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello Soup","topic":["T1","T2","T3","T4"]}
  {"messageID":3,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2","T3"]}
  {"messageID":4,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2"]}

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


        // Create ProducerContainer singleton
        final ProducerContainer producer = new ProducerContainer(args.ID, args.TransactionID);

        // Create a consumer that listens to incoming messages on a topic.
        ConsumerThread cThread = new ConsumerThread(producer, args.Topic, args.GroupID, args.ID);
        Thread t = new Thread(cThread);
        t.setName(args.Topic);
        t.start();

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
            }
        }
    }
}
