package Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import model.KafkaMessage;


/*Could use factory pattern here.*/
public class JsonEncoder {

    private Gson gson;

    public JsonEncoder() {
        this.gson = new GsonBuilder().create();
    }

    public String encode(KafkaMessage msg) {
        return gson.toJson(msg);
    }

    public KafkaMessage decode(String json) {
        KafkaMessage msg = null;
        try {
            msg = gson.fromJson(json, KafkaMessage.class);
        } catch (JsonSyntaxException e) {
            System.out.println("Was not able to decode");
        }
        return msg;
    }


}
