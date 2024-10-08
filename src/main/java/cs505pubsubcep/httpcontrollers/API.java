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
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:9000/api/checkmycep"
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
            responseMap.put("app_status_code", 1);

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
            String query = "DELETE VERTEX FROM patient";
            db.command(query);
            query = "DELETE VERTEX FROM hospital";
            db.command(query);
            query = "DELETE VERTEX FROM event";
            db.command(query);
            OutputSubscriber.alerts.clear();

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

            OResult item;
            List<String> contacts = new ArrayList<>();

            while (rs.hasNext()) {
                item = rs.next();
                if (item.isVertex() && !(item.getProperty("patient_mrn").equals(patient_mrn)))
                    contacts.add(item.getProperty("patient_mrn"));
            }

            for (int i = 0; i < contacts.size(); i++) {
                responseString += "\"" + contacts.get(i) + "\"" + (i==(contacts.size()-1) ? "": ",");
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

            List<String> events = new ArrayList<>();
            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.isVertex() && (item.getProperty("id") != null))
                    events.add(item.getProperty("id"));
            }

            for (int i = 0; i < events.size(); i++) {
                String query2 = "TRAVERSE inE(\"attended\"), outE(\"attended\"), inV(\"patient\"), outV(\"patient\") " +
                        "FROM (select from event where id = ?) " +
                        "WHILE $depth <= 2"; // get patients from event
                OResultSet rs2 = db.query(query2, events.get(i));
                responseString += "{\"" + events.get(i) + "\":[";

                List<String> attended = new ArrayList<>();
                while (rs2.hasNext()) {
                    OResult item2 = rs2.next();
                    if ((item2.getProperty("patient_mrn") != null) && !(item2.getProperty("patient_mrn").equals(patient_mrn)))
                        attended.add(item2.getProperty("patient_mrn"));
                }

                for (int j = 0; j < attended.size(); j++) {
                    responseString += "\"" + attended.get(j) + "\"" + (j==(attended.size()-1) ? "": ",");
                }

                responseString += "]}" + (i==(events.size()-1) ? "": ",");
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
                    "WHILE $depth <= 2 LIMIT 2000";
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
                    } else { // not vaccinated
                        if ((int)item.getProperty("patient_status") == 1)
                            in += 1;
                        else if ((int)item.getProperty("patient_status") == 2)
                            icu += 1;
                        else if ((int)item.getProperty("patient_status") == 3)
                            vent += 1;
                    }
                }
            }

            double in_total = in + vax_in;
            double icu_total = icu + vax_icu;
            double vent_total = vent + vax_vent;
            double in_vax = (in_total != 0 ? vax_in / in_total: 0);
            double icu_vax = (icu_total != 0 ? vax_icu / icu_total: 0);
            double vent_vax = (vent_total != 0 ? vax_vent / vent_total: 0);
            responseString = "{" +
                    "\"in-patient_count\": " + (int)in_total +
                    ",\"in-patient_vax\": " + in_vax +
                    ",\"icu-patient_count\": " + (int)icu_total +
                    ",\"icu-patient_vax\": " + icu_vax +
                    ",\"patient_vent_count\": " + (int)vent_total +
                    ",\"patient_vent_vax\": " + vent_vax +
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

            double in_total = in + vax_in;
            double icu_total = icu + vax_icu;
            double vent_total = vent + vax_vent;
            double in_vax = (in_total != 0 ? vax_in / in_total: 0);
            double icu_vax = (icu_total != 0 ? vax_icu / icu_total: 0);
            double vent_vax = (vent_total != 0 ? vax_vent / vent_total: 0);
            responseString = "{" +
                    "\"in-patient_count\": " + (int)in_total +
                    ",\"in-patient_vax\": " + in_vax +
                    ",\"icu-patient_count\": " + (int)icu_total +
                    ",\"icu-patient_vax\": " + icu_vax +
                    ",\"patient_vent_count\": " + (int)vent_total +
                    ",\"patient_vent_vax\": " + vent_vax +
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
}
