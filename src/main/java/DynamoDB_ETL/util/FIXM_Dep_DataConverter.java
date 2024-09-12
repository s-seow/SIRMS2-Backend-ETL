package DynamoDB_ETL.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

public class FIXM_Dep_DataConverter {

    public static String convertFIXMDepXmlToJson(String xmlStr) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(xmlStr)));

            JsonObject flight = new JsonObject();

            // Extract main flight attributes
            flight.addProperty("gufi", getTextContent(getElement(doc, "gufi", "http://www.fixm.aero/flight/4.1")));
            flight.addProperty("gufiOriginator", getAttribute(getElement(doc, "gufiOriginator", "http://www.fixm.aero/flight/4.1"), "name"));
            flight.addProperty("aircraftIdentification", getAttribute(getElement(doc, "flightIdentification", "http://www.fixm.aero/flight/4.1"), "aircraftIdentification"));

            // Extract departure details
            JsonObject departure = new JsonObject();
            Element departureElem = getElement(doc, "aerodrome", "http://www.fixm.aero/flight/4.1");
            String departureLocation = getAttribute(departureElem, "locationIndicator");
            departure.addProperty("departureAerodrome", departureLocation);
            departure.addProperty("actualTimeOfDeparture", getAttribute(getElement(doc, "departure", "http://www.fixm.aero/flight/4.1"), "actualTimeOfDeparture"));
            flight.add("departure", departure);

            // Extract arrival details
            JsonObject arrival = new JsonObject();
            Element arrivalElem = getElement(doc, "destinationAerodrome", "http://www.fixm.aero/flight/4.1");
            if (arrivalElem != null) {
                arrival.addProperty("destinationAerodrome", getAttribute(arrivalElem, "locationIndicator"));
            }
            flight.add("arrival", arrival);

            return new Gson().toJson(flight);


        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Helper functions
    private static String getAttribute(Element element, String attribute) {
        return element != null ? element.getAttribute(attribute) : null;
    }

    private static String getTextContent(Element element) {
        return element != null ? element.getTextContent() : null;
    }

    private static Element getElement(Document doc, String tagName, String namespaceURI) {
        NodeList nodeList = doc.getElementsByTagNameNS(namespaceURI, tagName);
        return nodeList.getLength() > 0 ? (Element) nodeList.item(0) : null;
    }
}
