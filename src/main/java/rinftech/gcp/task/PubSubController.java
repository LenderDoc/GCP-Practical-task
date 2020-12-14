package rinftech.gcp.task;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PubSubController {

    @Autowired
    private GcpTask gcpTask;

    @PostMapping("/")
    public ResponseEntity receiveMessage(@RequestBody String body) {

        JsonObject jsonAttributes = null;

        try {
            JsonObject jsonBody = JsonParser.parseString(body).getAsJsonObject();
            JsonObject message = jsonBody.getAsJsonObject("message");
            if (message != null) {
                jsonAttributes = message.getAsJsonObject("attributes");
            }
        } catch (JsonSyntaxException e) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            System.out.println(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        if (jsonAttributes == null) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            System.out.println(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        JsonElement jsonEventType = jsonAttributes.get("eventType");
        JsonElement jsonBucketId = jsonAttributes.get("bucketId");
        JsonElement jsonObjectId = jsonAttributes.get("objectId");

        // Validate the message is a Cloud Storage event.
        if (jsonEventType == null || jsonBucketId == null || jsonObjectId == null) {
            String msg = "Error: Invalid Cloud Storage notification: expected name and bucket properties";
            System.out.println(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        String eventType = jsonEventType.getAsString();
        String bucketId = jsonBucketId.getAsString();
        String objectId = jsonObjectId.getAsString();

        if (!eventType.equals("OBJECT_FINALIZE")) {
            String msg = "Error: Wrong Cloud Storage notification: OBJECT_FINALIZE expected";
            System.out.println(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }
        
        gcpTask.StreamToBigQuery(bucketId, objectId);
        
        return new ResponseEntity(HttpStatus.OK);
    }
}
