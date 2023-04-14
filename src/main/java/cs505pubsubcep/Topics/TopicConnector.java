package cs505pubsubcep.Topics;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505pubsubcep.CEP.OutputSubscriber;
import cs505pubsubcep.Launcher;
import io.siddhi.query.api.expression.condition.In;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,String>>>(){}.getType();
    final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();
    public HashMap<Integer, Integer> prevZips = new HashMap<>();
    public List<Integer> alerts = new ArrayList<>();

    //private String EXCHANGE_NAME = "patient_data";
    Map<String,String> config;

    public TopicConnector(Map<String,String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            //create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setPort(Integer.parseInt(config.get("port")));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            //create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
            hospitalListChannel(channel);
            vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
}

    private void patientListChannel(Channel channel) {
        try {

            System.out.println("Creating patient_list channel");

            String topicName = "patient_list";

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Paitent List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received Patient List Batch'" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                List<TestingData> incomingList = gson.fromJson(message, typeListTestingData);
                HashMap<Integer, Integer> zips = new HashMap<>();
                alerts.clear();
                for (TestingData testingData : incomingList) {
//                    System.out.println("*Java Class*");
//                    System.out.println("\ttesting_id = " + testingData.testing_id);
//                    System.out.println("\tpatient_name = " + testingData.patient_name);
//                    System.out.println("\tpatient_mrn = " + testingData.patient_mrn);
//                    System.out.println("\tpatient_zipcode = " + testingData.patient_zipcode);
//                    System.out.println("\tpatient_status = " + testingData.patient_status);
//                    System.out.println("\tcontact_list = " + testingData.contact_list);
//                    System.out.println("\tevent_list = " + testingData.event_list);

                    if (zips.get(testingData.patient_zipcode) == null) {
                        zips.put(testingData.patient_zipcode, 1);
                    } else {
                        zips.put(testingData.patient_zipcode, zips.get(testingData.patient_zipcode)+1);
                    }
//                    System.out.println("\n\n\n" + zips.get(testingData.patient_zipcode).toString() + " " + testingData.patient_zipcode + "\n\n\n");
                }

                //maybe access hash while in loop above
                OutputSubscriber.alerts.clear();
                Map<String, Integer> send = new HashMap<>();
                if (!prevZips.isEmpty()) {
                    for (Integer zip : zips.keySet()) {
                        if (prevZips.get(zip) != null) {
//                            if (zips.get(zip) >= prevZips.get(zip) * 2) {
//                                alerts.add(zip);
//                                System.out.println("====================\n\n\nALERT" + zip + "\n\n\n====================");
//                            }
                            send.clear();
                            send.put("zip_code", zip);
                            send.put("current_count", zips.get(zip));
                            send.put("prev_count", prevZips.get(zip) * 2);
//                            System.out.println("\n" + zips.get(zip) + "\t" + (prevZips.get(zip)) + "\n");
                            Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(send));
                        }
                    }
                }
                prevZips = (HashMap) zips.clone();

//                System.out.println("====================\n\n\nTEST\n\n\n====================");

//                prevZips = new HashMap<Integer, Integer>(zips);

//                List<Map<String,String>> incomingListMap = gson.fromJson(message, typeOf);
//                for(Map<String,String> map : incomingListMap) {
//                    System.out.println("INPUT CEP EVENT: " +  map);
//                    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
//                }
//                send.put("zip_code", alerts.size());
//                Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(send));
                System.out.println("");
                System.out.println("");
//                Launcher.cepEngine.input(Launcher.inputStreamName, message);

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received Hospital List Batch'" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received Vax Batch'" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

}
