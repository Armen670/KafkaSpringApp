package com.example;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/post-message")
public class KafkaController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "postedmessages";

    @PostMapping
    public ResponseEntity<String> postMessage(@RequestBody Map<String, String> requestBody) {
        try {
            // Извлекаем msg_id
            String msgId = requestBody.get("msg_id");
            if (msgId == null) {
                return ResponseEntity.badRequest().body("Field 'msg_id' is missing");
            }

            // Подготовка данных
            Map<String, Object> kafkaMessage = new HashMap<>();
            kafkaMessage.put("msg_id", msgId);
            kafkaMessage.put("timestamp", Instant.now().toEpochMilli());
            kafkaMessage.put("method", "POST");
            kafkaMessage.put("url", "/post-message");

            // Отправка в Kafka
            kafkaTemplate.send(TOPIC, kafkaMessage.toString());

            return ResponseEntity.ok("Message sent to Kafka successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Failed to send message to Kafka");
        }
    }
}
