package cs505pubsubcep;

import cs505pubsubcep.CEP.CEPEngine;
import cs505pubsubcep.Topics.TopicConnector;
import cs505pubsubcep.graphDB.GraphDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class Launcher {

    public static final String API_SERVICE_KEY = "12353015"; //Change this to your student id
    public static final int WEB_PORT = 9000;
    public static String inputStreamName = null;
    public static long accessCount = -1;

    public static GraphDBEngine graphDBEngine;
    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;

    public static void main(String[] args) throws IOException {

        //startig DB/CEP init

        //READ CLASS COMMENTS BEFORE USING
        graphDBEngine = new GraphDBEngine();

        System.out.println("Starting CEP...");

        cepEngine = new CEPEngine();


        //START MODIFY
        inputStreamName = "PatientInStream";
//        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";
        String inputStreamAttributesString = "zip_code string, current_count int, prev_count int";


        String outputStreamName = "PatientOutStream";
//        String outputStreamAttributesString = "patient_status_code string, count long";
        String outputStreamAttributesString = "zip_code string, current_count int, prev_count int";


//        String queryString = " " +
//                "from PatientInStream " +
//                "select patient_status_code, count() as count " +
//                "group by patient_status_code " +
//                "insert into PatientOutStream; ";
        String queryString = " " +
                "from PatientInStream#window.lengthBatch(1) " +
                "select zip_code, current_count, prev_count " +
                "having current_count >= prev_count " +
                "insert into PatientOutStream; ";

        //END MODIFY

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");



        //starting pateint_data collector
        Map<String,String> message_config = new HashMap<>();
//        message_config.put("hostname","vbu231.cs.uky.edu"); //Fill config for your team in
        message_config.put("hostname", "localhost");
        message_config.put("port","5672"); //9099
        message_config.put("username","team_7"); // actually team 7
        message_config.put("password","myPassCS505");
        message_config.put("virtualhost","7");

        topicConnector = new TopicConnector(message_config);
        topicConnector.connect();

        //Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505pubsubcep.httpcontrollers");

//        .register(AuthenticationFilter.class);

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
