import Runnables.ConsumerThread;
import Runnables.ProducerContainer;
import Serializer.JsonEncoder;
import com.beust.jcommander.JCommander;
import constants.Args;

import java.util.Scanner;

// Typical message to send, remember to increase the MessageID
// {"messageID":1,"senderID":1,"messageType":"CLIENT","value":"this is a test","topic":["test","mest"]}
// {"messageID":1,"clientID":1,"messageType":"CLIENT","value":"Hello","topic":["T1","T2"]}

// Command to run jar
// java -jar 0.jar -Topic T1 -TransactionID 1 -GroupID 1 -ID 1
// java -jar 0.jar -Topic T2 -TransactionID 2 -GroupID 2 -ID 2

// Notes:
// Every "AtomicNode" is a detached process, what this mean is that they do not know of any other topic or any other
// AtomicNodes, A node might be the only one, or there might be several others. Since this is a detached content based
// atomic multicast system everything should be detached until needed.
// until a client tells them about it, after they have agreed on the message the
// "AtomicNode" forgets about that topic, as the topic-relation only lives inside the message.

// I think we need four stages for agreeing on total order.
// 1. Receive from client
// 2. Send to other topics in the list
// 3. Agree on the highest timestamp.
// 4. Deliver the message.

// I dont think we need to rebuild any state, as if a AtomicNode has crashed and or died, it wont accept any value
// The only reason to rebuild something might be because of a crash on delivery.
//      - This can be fixed by replaying (not committing) any messages, but how do we do with the other nodes? msg them?
//          - Can check for latest offset of other nodes and our node, and do some magic?

/*
  {"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2"]}
  {"messageID":1,"clientID":2,"messageType":"NotifyMessage","value":"Hello","topic":["T1","T2"]}

  {"messageID":1,"clientID":2,"messageType":"AckMessage","value":"Hello","topic":["T1","T2"]}

*/


/*
 * If a node crashes, maybe another node can spawn a new process, or take over the responability of the crashed node.
 * */

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