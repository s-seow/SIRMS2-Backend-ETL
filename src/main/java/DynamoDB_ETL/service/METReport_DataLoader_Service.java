package DynamoDB_ETL.service;

import DynamoDB_ETL.util.METReport_DataConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class METReport_DataLoader_Service {

    @Autowired
    private DynamoDbClient dynamoDbClient;

    public void processMessageContent(String jsonContent, String logTimestamp, String jmsMessageID, String jmsDestination) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonContent);

            String id = rootNode.path("id").asText();
            String metarData = rootNode.path("properties").path("content").path("value").asText();

            String jsonStr = METReport_DataConverter.convertMETDataToJson(id, metarData);

            if (jsonStr != null) {
                processMETReportAndStoreInDynamoDB(jsonStr, logTimestamp, jmsMessageID, jmsDestination);
            } else {
                System.out.println("METAR data structure not recognized.");
            }

        } catch (Exception e) {
            System.err.println("Failed to process message content: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processMETReportAndStoreInDynamoDB(String jsonStr, String logTimestamp, String messageID, String messageDestination) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonStr);

            if (!jsonNode.has("id")) {
                throw new RuntimeException("Missing required fields: id");
            }

            String datePart = logTimestamp.substring(0, 10);  // Extract date from logTimestamp (e.g., 2024-10-18)

            String dateTime = jsonNode.has("dateTime") ? jsonNode.get("dateTime").asText() : null;

            String refinedDateTime = null;
            if (dateTime != null && dateTime.length() == 6) {
                // ddHHmm format (day, hour, minute)
                String day = dateTime.substring(0, 2);
                String hour = dateTime.substring(2, 4);
                String minute = dateTime.substring(4, 6);

                String second = "00";
                String millisecond = "000";

                LocalDate logDate = LocalDate.parse(datePart);

                int dayOfMonth = Integer.parseInt(day);
                if (dayOfMonth != logDate.getDayOfMonth()) {
                    logDate = logDate.withDayOfMonth(dayOfMonth);
                }

                LocalTime time = LocalTime.of(
                        Integer.parseInt(hour),
                        Integer.parseInt(minute),
                        Integer.parseInt(second),
                        Integer.parseInt(millisecond) * 1_000_000
                );

                LocalDateTime localDateTime = LocalDateTime.of(logDate, time);

                refinedDateTime = localDateTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

            } else {
                // Fallback to logTimestamp if no valid dateTime from METAR
                refinedDateTime = logTimestamp;
            }

            // Add refinedDateTime, messageID, and messageDestination to the JSON
            if (jsonNode instanceof ObjectNode) {
                ObjectNode objectNode = (ObjectNode) jsonNode;
                if (refinedDateTime != null) {
                    objectNode.put("dateTime", refinedDateTime);
                }
                if (messageID != null) {
                    objectNode.put("messageID", messageID);
                }
                if (messageDestination != null) {
                    objectNode.put("messageDestination", messageDestination);
                }
                if (logTimestamp != null) {
                    objectNode.put("logTimestamp", logTimestamp);
                }
            }

            // Convert the JSON to DynamoDB attribute values
            Map<String, AttributeValue> item = convertJsonNodeToAttributeValue(jsonNode);

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName("METReport_FlightData")
                    .item(item)
                    .build();

            System.out.println("Final JSON to insert: " + jsonNode);
            dynamoDbClient.putItem(putItemRequest);

            System.out.printf("Item successfully inserted: %s%n", item);
            System.out.println();

        } catch (Exception e) {
            System.err.println("Failed to process JSON and store in DynamoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private Map<String, AttributeValue> convertJsonNodeToAttributeValue(JsonNode jsonNode) {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();

        jsonNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isObject()) {
                attributeValueMap.put(key, AttributeValue.builder().m(convertJsonNodeToAttributeValue(value)).build());
            } else if (value.isArray()) {
                attributeValueMap.put(key, AttributeValue.builder().l(convertJsonArrayToAttributeValueList(value)).build());
            } else {
                attributeValueMap.put(key, AttributeValue.builder().s(value.asText()).build());
            }
        });

        return attributeValueMap;
    }

    private List<AttributeValue> convertJsonArrayToAttributeValueList(JsonNode jsonArray) {
        List<AttributeValue> attributeValueList = new java.util.ArrayList<>();

        for (JsonNode jsonNode : jsonArray) {
            if (jsonNode.isObject()) {
                attributeValueList.add(AttributeValue.builder().m(convertJsonNodeToAttributeValue(jsonNode)).build());
            } else if (jsonNode.isArray()) {
                attributeValueList.add(AttributeValue.builder().l(convertJsonArrayToAttributeValueList(jsonNode)).build());
            } else {
                attributeValueList.add(AttributeValue.builder().s(jsonNode.asText()).build());
            }
        }

        return attributeValueList;
    }
}
