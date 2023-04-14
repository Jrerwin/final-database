package cs505pubsubcep.CEP;

import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.List;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    public static Integer alert = 0;
    public static List<Integer> alerts = new ArrayList<>();

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");
            String[] sstr = String.valueOf(msg).split(":");
            String[] outval = sstr[2].split("\"");
            int val = Integer.parseInt(outval[1]);
            alerts.add(val);
            System.out.println("\nAdded: " + val + "!\n");
//            alert = Integer.parseInt(outval[0]);
//            System.out.println(alert);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
