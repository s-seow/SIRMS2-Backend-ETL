package DynamoDB_ETL.service;

import DynamoDB_ETL.util.METReport_WSSL_DataConverter;
import DynamoDB_ETL.util.METReport_WSSS_DataConverter;
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

    public void processMessageContent(String xmlContent, String logTimestamp, String jmsMessageID, String jmsDestination) {

        String jsonStr = null;

        if (jmsDestination.endsWith("wsss")) {
            jsonStr = METReport_WSSS_DataConverter.convertWSSSMETDataToJson(xmlContent);
        } else if (jmsDestination.endsWith("wssl")) {
            jsonStr = METReport_WSSL_DataConverter.convertWSSLMETDataToJson(xmlContent);
        }

        if (jsonStr != null) {
            processMETReportAndStoreInDynamoDB(jsonStr, logTimestamp, jmsMessageID, jmsDestination);
        } else {
            System.out.println("XML structure not recognized.");
        }
    }

    private void processMETReportAndStoreInDynamoDB(String jsonStr, String logTimestamp, String messageID, String messageDestination) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonStr);

            if (jsonNode instanceof ObjectNode) {
                ObjectNode objectNode = (ObjectNode) jsonNode;

                if (!objectNode.has("id") || !objectNode.has("dateTime")) {
                    throw new RuntimeException("Missing required fields: id or dateTime");
                }

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

            Map<String, AttributeValue> item = convertJsonNodeToAttributeValue(jsonNode);

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName("METReport_FlightData")
                    .item(item)
                    .build();

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

        if (jsonNode.isObject()) {
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
        }

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
