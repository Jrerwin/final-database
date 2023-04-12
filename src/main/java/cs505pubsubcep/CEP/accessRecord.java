package cs505pubsubcep.CEP;

public class accessRecord {
    String remote_ip;
    long timestamp;

    public accessRecord(String remote_ip, long timestamp) {
        this.remote_ip = remote_ip;
        this.timestamp = timestamp;
    }
    public String getRemoteIp() {return remote_ip;}
    public long getTs() {return timestamp;}

    @Override
    public String toString() {
        return "remote_ip:" + remote_ip + " timestamp:" + timestamp;
    }
}

/*
{
        "remote_ip":  "127.0.0.1",							 #A
        "timestamp": "1576600245000",						 #B
        }
*/