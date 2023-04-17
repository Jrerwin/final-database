package cs505pubsubcep.httpcontrollers;

import com.google.gson.Gson;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import cs505pubsubcep.CEP.OutputSubscriber;
import cs505pubsubcep.CEP.accessRecord;
import cs505pubsubcep.Launcher;
import org.apache.tapestry5.json.JSONObject;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    //check local
    //curl --header "X-Auth-API-key:1234" "http://localhost:9000/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    //curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeam(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            Map<String,Object> responseMap = new HashMap<>();
            List<Integer> membersList = new ArrayList<>();

            membersList.add(12353015);
            membersList.add(12435352);
            membersList.add(12352768);
            responseMap.put("team_name", "Birch Tree Club");
            responseMap.put("members", membersList);
            responseMap.put("app_status_code", 0);

            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@HeaderParam("X-Auth-API-Key") String authKey) {
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("dbproject", "root", "password");
        String responseString = "{}";

        try {
            Launcher.graphDBEngine.clearDB(db);
            responseString = "{ \"reset_status_code\": 1}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();
            responseString = "{ \"reset_status_code\": 0}";
//            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            List<Integer> list = OutputSubscriber.alerts;
            responseString = "{\"ziplist\": [";
            for (int i = 0; i < list.size(); i++) {
                responseString += list.get(i);
                if (i != list.size() -1) {
                    responseString += ",";
                }
            }
            responseString += "]}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertState(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            int alert = OutputSubscriber.alerts.size();
            if (alert >= 5) {
                responseString = "{\"state_status\": 1}";
            }
            else
            {
                responseString = "{\"state_status\": 0}";
            }
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getContacts(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String patient_mrn) {
        String responseString = "{}";
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("dbproject", "root", "password");

        try {
            responseString = "{\"contactlist\": [";
            String query = "TRAVERSE inE(\"contact\"), outE(\"contact\"), inV(\"patient\"), outV(\"patient\") " +
                    "FROM (select from patient where patient_mrn = ?) " +
                    "WHILE $depth <= 2";
            OResultSet rs = db.query(query, patient_mrn);

            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.isVertex()) {
                    if (!(item.getProperty("patient_mrn").equals(patient_mrn))) {
                        responseString += item.getProperty("patient_mrn");
                        if (rs.hasNext()) {
                            responseString += ",";
                        }
                    }
                }
            }
            responseString += "]}";
            rs.close();

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/getpossiblecontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String patient_mrn) {
        String responseString = "{}";
        try {
            responseString = "{\"contactlist\": []}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("hospital_id") String hospital_id) {
        String responseString = "{}";
        try {
            responseString = "{" +
                    "\"in-patient_count\": 0" +
                    "\"in-patient_vax\": 0" +
                    "\"icu-patient_count\": 0" +
                    "\"icu-patient_vax\": 0" +
                    "\"patient_vent_count\": 0" +
                    "\"patient_vent_vax\": 0" +
                    "}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    @GET
    @Path("/getpatientstatus/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllPatientStatus(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            responseString = "{" +
                    "\"in-patient_count\": 0" +
                    "\"in-patient_vax\": 0" +
                    "\"icu-patient_count\": 0" +
                    "\"icu-patient_vax\": 0" +
                    "\"patient_vent_count\": 0" +
                    "\"patient_vent_vax\": 0" +
                    "}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


//    @GET
//    @Path("/checkmycep")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response checkMyEndpoint(@HeaderParam("X-Auth-API-Key") String authKey) {
//        String responseString = "{}";
//        try {
//
//            //get remote ip address from request
//            String remoteIP = request.get().getRemoteAddr();
//            //get the timestamp of the request
//            long access_ts = System.currentTimeMillis();
//            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);
//
//            Map<String,String> responseMap = new HashMap<>();
//            if(Launcher.cepEngine != null) {
//
//                    responseMap.put("success", Boolean.TRUE.toString());
//                    responseMap.put("status_desc","CEP Engine exists");
//
//            } else {
//                responseMap.put("success", Boolean.FALSE.toString());
//                responseMap.put("status_desc","CEP Engine is null!");
//            }
//
//            responseString = gson.toJson(responseMap);
//
//
//        } catch (Exception ex) {
//
//            StringWriter sw = new StringWriter();
//            ex.printStackTrace(new PrintWriter(sw));
//            String exceptionAsString = sw.toString();
//            ex.printStackTrace();
//
//            return Response.status(500).entity(exceptionAsString).build();
//        }
//        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
//    }
//
//    @GET
//    @Path("/getaccesscount")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
//        String responseString = "{}";
//        try {
//
//            //get remote ip address from request
//            String remoteIP = request.get().getRemoteAddr();
//            //get the timestamp of the request
//            long access_ts = System.currentTimeMillis();
//            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);
//
//            //generate event based on access
//            String inputEvent = gson.toJson(new accessRecord(remoteIP,access_ts));
//            System.out.println("inputEvent: " + inputEvent);
//
//            //send input event to CEP
//            Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);
//
//            //generate a response
//            Map<String,String> responseMap = new HashMap<>();
//            responseMap.put("accesscoint",String.valueOf(Launcher.accessCount));
//            responseString = gson.toJson(responseMap);
//
//        } catch (Exception ex) {
//
//            StringWriter sw = new StringWriter();
//            ex.printStackTrace(new PrintWriter(sw));
//            String exceptionAsString = sw.toString();
//            ex.printStackTrace();
//
//            return Response.status(500).entity(exceptionAsString).build();
//        }
//        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
//    }


}
