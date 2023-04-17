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
import io.siddhi.query.api.expression.condition.In;
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
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("dbproject", "root", "password");

        try {
            responseString = "{\"contactlist\": [";
            String query = "TRAVERSE inE(\"attended\"), outE(\"attended\"), inV(), outV() " +
                    "FROM (select from patient where patient_mrn = ?) " +
                    "WHILE $depth <= 2";
            OResultSet rs = db.query(query, patient_mrn); // get events

            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.isVertex() && item.getProperty("patient_mrn") == null) { //  is not a patient, should be event
                    String query2 = "TRAVERSE inE(\"attended\"), outE(\"attended\"), inV(\"patient\"), outV(\"patient\") " +
                            "FROM (select from event where id = ?) " +
                            "WHILE $depth <= 2"; // get patients from event
                    OResultSet rs2 = db.query(query2, item.getProperty("id").toString());
                    responseString += item.getProperty("id") + ":[";
                    while (rs2.hasNext()) {
                        OResult item2 = rs2.next();
                        if ((item2.getProperty("patient_mrn") != null) && !item2.getProperty("patient_mrn").equals(patient_mrn)) {
                            responseString += item2.getProperty("patient_mrn");
                            if (rs2.hasNext()) {
                                responseString += ",";
                            }
                        }
                    }
                    responseString += "]";
                    if (rs.hasNext()) {
                        responseString += ",";
                    }
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
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("hospital_id") String hospital_id) {
        String responseString = "{}";
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("dbproject", "root", "password");

        try {
            int in = 0;
            int vax_in = 0;
            int icu = 0;
            int vax_icu = 0;
            int vent = 0;
            int vax_vent = 0;
            String query = "TRAVERSE inE(\"contains\"), outE(\"contains\"), inV(\"patient\"), outV(\"patient\") " +
                    "FROM (select from hospital where hospital_id = ?) " +
                    "WHILE $depth <= 2";
            OResultSet rs = db.query(query, hospital_id);

            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.getProperty("patient_mrn") != null) { // is patient
                    if (item.getProperty("vaccination_id") != null) { // is vaccinated
                        if ((int)item.getProperty("patient_status") == 1)
                            vax_in += 1;
                        else if ((int)item.getProperty("patient_status") == 2)
                            vax_icu += 1;
                        else if ((int)item.getProperty("patient_status") == 3)
                            vax_vent += 1;
                    } else {
                        if ((int)item.getProperty("patient_status") == 1)
                            in += 1;
                        else if ((int)item.getProperty("patient_status") == 2)
                            icu += 1;
                        else if ((int)item.getProperty("patient_status") == 3)
                            vent += 1;
                    }
                }
            }

            responseString = "{" +
                    "\"in-patient_count\": " + in +
                    ",\"in-patient_vax\": " + vax_in +
                    ",\"icu-patient_count\": " + icu +
                    ",\"icu-patient_vax\": " + vax_icu +
                    ",\"patient_vent_count\": " + vent +
                    ",\"patient_vent_vax\": " + vax_vent +
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
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("dbproject", "root", "password");

        try {
            int in = 0;
            int vax_in = 0;
            int icu = 0;
            int vax_icu = 0;
            int vent = 0;
            int vax_vent = 0;
            String query = "TRAVERSE inE(\"contains\"), outE(\"contains\"), inV(\"patient\"), outV(\"patient\") " +
                    "FROM (select from hospital) " +
                    "WHILE $depth <= 2";
            OResultSet rs = db.query(query);

            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.getProperty("patient_mrn") != null) { // is patient
                    if (item.getProperty("vaccination_id") != null) { // is vaccinated
                        if ((int)item.getProperty("patient_status") == 1)
                            vax_in += 1;
                        else if ((int)item.getProperty("patient_status") == 2)
                            vax_icu += 1;
                        else if ((int)item.getProperty("patient_status") == 3)
                            vax_vent += 1;
                    } else {
                        if ((int)item.getProperty("patient_status") == 1)
                            in += 1;
                        else if ((int)item.getProperty("patient_status") == 2)
                            icu += 1;
                        else if ((int)item.getProperty("patient_status") == 3)
                            vent += 1;
                    }
                }
            }

            responseString = "{" +
                    "\"in-patient_count\": " + in +
                    ",\"in-patient_vax\": " + vax_in +
                    ",\"icu-patient_count\": " + icu +
                    ",\"icu-patient_vax\": " + vax_icu +
                    ",\"patient_vent_count\": " + vent +
                    ",\"patient_vent_vax\": " + vax_vent +
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
