package cs505pubsubcep.Topics;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505pubsubcep.CEP.OutputSubscriber;
import cs505pubsubcep.Launcher;
import cs505pubsubcep.graphDB.GraphDBEngine;
import io.siddhi.query.api.expression.condition.In;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ObjDoubleConsumer;


public class TopicConnector {

    private Gson gson;
    final Type typeOfListMap = new TypeToken<List<Map<String,String>>>(){}.getType();

    final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();
    public HashMap<Integer, Integer> prevZips = new HashMap<>();
    public List<Integer> alerts = new ArrayList<>();

    //private String EXCHANGE_NAME = "patient_data";
    Map<String,String> config;
    ODatabaseSession db = Launcher.graphDBEngine.db;

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

                    OClass patient = db.getClass("patient");
                    if (patient == null) {
                        patient = db.createVertexClass("patient");
                    }
                    OVertex patient_vertex = Launcher.graphDBEngine.createPatient(db, testingData.patient_mrn);
//                    OVertex patient_vertex = db.newVertex("patient");
//                    patient_vertex.setProperty("patient_mrn", testingData.patient_mrn);
//                    patient_vertex.setProperty("patient_name", testingData.patient_name);
//                    patient_vertex.setProperty("patient_status", testingData.patient_status);
//                    patient_vertex.setProperty("patient_zipcode", testingData.patient_zipcode);
//                    for (String contact : testingData.contact_list) {
//                        String query = "select from patient where patient_mrn = ?";
//                        OResultSet rs = db.query(query, testingData.patient_mrn);
//                        OVertex contact_obj;
//                        if (!rs.hasNext()) { // Need to make new vertex
//                            contact_obj = db.newVertex("patient");
//                            contact_obj.setProperty("patient_mrn", contact);
//                            contact_obj.save();
//                        } else {
//                            OResult res = rs.next();
//                            contact_obj = (OVertex)res;
//                        }
//
//                        OEdge contact_edge = db.newEdge(patient_vertex, contact_obj);
//                        contact_edge.save();
//                    }
//                    patient_vertex.save();

                    if (zips.get(testingData.patient_zipcode) == null) {
                        zips.put(testingData.patient_zipcode, 1);
                    } else {
                        zips.put(testingData.patient_zipcode, zips.get(testingData.patient_zipcode)+1);
                    }
                }

                //maybe access hash while in loop above
                OutputSubscriber.alerts.clear();
                Map<String, Integer> send = new HashMap<>();
                if (!prevZips.isEmpty()) {
                    for (Integer zip : zips.keySet()) {
                        if (prevZips.get(zip) != null) {
                            send.clear();
                            send.put("zip_code", zip);
                            send.put("current_count", zips.get(zip));
                            send.put("prev_count", prevZips.get(zip) * 2);
                            Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(send));
                        }
                    }
                }
                prevZips = (HashMap) zips.clone();
                System.out.println("");
                System.out.println("");
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

                //new message
                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> hospitalData : incomingList) {
                    int hospital_id = Integer.parseInt(hospitalData.get("hospital_id"));
                    String patient_name = hospitalData.get("patient_name");
                    String patient_mrn = hospitalData.get("patient_mrn");
                    int patient_status = Integer.parseInt(hospitalData.get("patient_status"));
                    //do something with each each record.
                }

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
