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

            // Add the logTimestamp, messageID, and messageDestination to the JSON
            if (jsonNode instanceof ObjectNode) {
                ObjectNode objectNode = (ObjectNode) jsonNode;
                if (logTimestamp != null) {
                    objectNode.put("logTimestamp", logTimestamp);
                }
                if (messageID != null) {
                    objectNode.put("messageID", messageID);
                }
                if (messageDestination != null) {
                    objectNode.put("messageDestination", messageDestination);
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
