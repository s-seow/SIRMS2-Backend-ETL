package DynamoDB_ETL.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.Base64;

public class IWXXM_DataConverter {

    public static String convertIWXXMXmlToJson(String xmlContent, String id, String logTimestamp) {
        try {
            // Parse to extract Base64 content
            JsonObject jsonObject = JsonParser.parseString(xmlContent).getAsJsonObject();
            String base64Content = jsonObject
                    .getAsJsonObject("properties")
                    .getAsJsonObject("content")
                    .get("value")
                    .getAsString();

            // Decode Base64 content
            String decodedXml = new String(Base64.getDecoder().decode(base64Content));

            // Convert the XML content to a JSON object
            JsonObject iwxxmDataJson = convertXmlToJson(decodedXml);

            // Create the final JSON structure
            JsonObject finalJsonObject = new JsonObject();
            finalJsonObject.addProperty("id", id);
            finalJsonObject.addProperty("logTimestamp", logTimestamp);
            finalJsonObject.add("decodedData", iwxxmDataJson);

            return finalJsonObject.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static JsonObject convertXmlToJson(String xmlContent) {
        JsonObject jsonObject = new JsonObject();
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xmlContent));
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            jsonObject = convertNodeToJson(doc.getDocumentElement());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    private static JsonObject convertNodeToJson(Node node) {
        JsonObject jsonObject = new JsonObject();

        // Add attributes
        if (node.hasAttributes()) {
            NamedNodeMap nodeMap = node.getAttributes();
            for (int i = 0; i < nodeMap.getLength(); i++) {
                Node attribute = nodeMap.item(i);
                jsonObject.addProperty(attribute.getNodeName(), attribute.getNodeValue());
            }
        }

        // Add child nodes
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node childNode = nodeList.item(i);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                JsonObject childJson = convertNodeToJson(childNode);
                if (jsonObject.has(childNode.getNodeName())) {
                    // If a node with this name already exists, convert it to an array
                    JsonElement existingElement = jsonObject.get(childNode.getNodeName());
                    if (existingElement.isJsonArray()) {
                        existingElement.getAsJsonArray().add(childJson);
                    } else {
                        JsonObject firstChild = existingElement.getAsJsonObject();
                        jsonObject.remove(childNode.getNodeName());
                        JsonArray array = new JsonArray();
                        array.add(firstChild);
                        array.add(childJson);
                        jsonObject.add(childNode.getNodeName(), array);
                    }
                } else {
                    jsonObject.add(childNode.getNodeName(), childJson);
                }
            } else if (childNode.getNodeType() == Node.TEXT_NODE && !childNode.getNodeValue().trim().isEmpty()) {
                jsonObject.addProperty("value", childNode.getNodeValue().trim());
            }
        }
        return jsonObject;
    }
}
