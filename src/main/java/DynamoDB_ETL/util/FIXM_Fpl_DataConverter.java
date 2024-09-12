package DynamoDB_ETL.util;

import com.google.gson.JsonObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class FIXM_Fpl_DataConverter {

    private static final Map<String, String> NAMESPACE = new HashMap<String, String>() {{
        put("fx", "http://www.fixm.aero/flight/4.1");
        put("fb", "http://www.fixm.aero/base/4.1");
    }};

    public static String convertFIXMFplXmlToJson(String xmlStr) {
        Document document;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            document = builder.parse(new InputSource(new StringReader(xmlStr)));
        } catch (Exception e) {
            System.err.println("Error parsing XML: " + e.getMessage());
            return null;
        }

        Element flightElem = document.getDocumentElement();
        JsonObject flight = new JsonObject();

        // Extract main flight attributes
        flight.addProperty("gufi", getText(getElement(flightElem, "fx:gufi")));
        flight.addProperty("aircraftIdentification", getAttribute(getElement(flightElem, "fx:flightIdentification"), "aircraftIdentification"));
        flight.addProperty("flightType", getAttribute(flightElem, "flightType"));
        flight.addProperty("gufiOriginator", getAttribute(getElement(flightElem, "fx:gufiOriginator"), "name"));
        flight.addProperty("operator", getAttribute(getElement(flightElem, "fb:operatingOrganization"), "name"));
        flight.addProperty("remarks", getAttribute(flightElem, "remarks"));

        // Extract aircraft details
        Element aircraftElem = getElement(flightElem, "fx:aircraft");
        if (aircraftElem != null) {
            JsonObject aircraft = new JsonObject();
            aircraft.addProperty("aircraftAddress", getAttribute(aircraftElem, "aircraftAddress"));
            aircraft.addProperty("aircraftApproachCategory", getAttribute(aircraftElem, "aircraftApproachCategory"));
            aircraft.addProperty("registration", getAttribute(aircraftElem, "registration"));
            aircraft.addProperty("wakeTurbulence", getAttribute(aircraftElem, "wakeTurbulence"));
            Element aircraftTypeElem = getElement(aircraftElem, "fx:type");
            if (aircraftTypeElem != null) {
                String aircraftType = getAttribute(aircraftTypeElem, "icaoAircraftTypeDesignator");
                aircraft.addProperty("aircraftType", aircraftType);
            }

            // Extract capabilities
            Element capabilitiesElem = getElement(aircraftElem, "fx:capabilities");
            if (capabilitiesElem != null) {
                JsonObject capabilities = new JsonObject();
                capabilities.addProperty("standardCapabilities", getAttribute(capabilitiesElem, "standardCapabilities"));

                Element communicationElem = getElement(capabilitiesElem, "fx:communication");
                if (communicationElem != null) {
                    JsonObject communication = new JsonObject();
                    communication.addProperty("otherDatalinkCapabilities", getAttribute(communicationElem, "otherDatalinkCapabilities"));
                    communication.addProperty("selectiveCallingCode", getAttribute(communicationElem, "selectiveCallingCode"));
                    communication.addProperty("communicationCapabilityCode", getTextContent(communicationElem, "fx:communicationCapabilityCode"));
                    communication.addProperty("datalinkCommunicationCapabilityCode", getTextContent(communicationElem, "fx:datalinkCommunicationCapabilityCode"));
                    capabilities.add("communication", communication);
                }

                Element navigationElem = getElement(capabilitiesElem, "fx:navigation");
                if (navigationElem != null) {
                    JsonObject navigation = new JsonObject();
                    navigation.addProperty("otherNavigationCapabilities", getAttribute(navigationElem, "otherNavigationCapabilities"));
                    navigation.addProperty("navigationCapabilityCode", getTextContent(navigationElem, "fx:navigationCapabilityCode"));
                    navigation.addProperty("performanceBasedCode", getTextContent(navigationElem, "fx:performanceBasedCode"));
                    capabilities.add("navigation", navigation);
                }

                Element surveillanceElem = getElement(capabilitiesElem, "fx:surveillance");
                if (surveillanceElem != null) {
                    JsonObject surveillance = new JsonObject();
                    surveillance.addProperty("otherSurveillanceCapabilities", getAttribute(surveillanceElem, "otherSurveillanceCapabilities"));
                    surveillance.addProperty("surveillanceCapabilityCode", getTextContent(surveillanceElem, "fx:surveillanceCapabilityCode"));
                    capabilities.add("surveillance", surveillance);
                }
                aircraft.add("capabilities", capabilities);
            }
            flight.add("aircraft", aircraft);
        }

        // Extract arrival details
        Element arrivalElem = getElement(flightElem, "fx:arrival");
        if (arrivalElem != null) {
            JsonObject arrival = new JsonObject();
            arrival.addProperty("destinationAerodrome", getAttribute(getElement(arrivalElem, "fx:destinationAerodrome"), "locationIndicator"));
            arrival.addProperty("destinationAerodromeAlternate", getAttribute(getElement(arrivalElem, "fx:destinationAerodromeAlternate"), "locationIndicator"));
            flight.add("arrival", arrival);
        }

        // Extract departure details
        Element departureElem = getElement(flightElem, "fx:departure");
        if (departureElem != null) {
            JsonObject departure = new JsonObject();
            departure.addProperty("estimatedOffBlockTime", getAttribute(departureElem, "estimatedOffBlockTime"));
            departure.addProperty("departureAerodrome", getAttribute(getElement(departureElem, "fx:aerodrome"), "locationIndicator"));
            flight.add("departure", departure);
        }

        // Extract filed details
        Element filedElem = getElement(flightElem, "fx:filed");
        if (filedElem != null) {
            JsonObject filed = new JsonObject();
            JsonObject routeInformation = new JsonObject();

            Element routeInformationElem = getElement(filedElem, "fx:routeInformation");
            if (routeInformationElem != null) {
                routeInformation.addProperty("flightRulesCategory", getAttribute(routeInformationElem, "flightRulesCategory"));
                routeInformation.addProperty("routeText", getAttribute(routeInformationElem, "routeText"));
                routeInformation.addProperty("totalEstimatedElapsedTime", getAttribute(routeInformationElem, "totalEstimatedElapsedTime"));

                Element cruisingLevelElem = getElement(routeInformationElem, "fb:flightLevel");
                if (cruisingLevelElem != null) {
                    JsonObject cruisingLevel = new JsonObject();
                    cruisingLevel.addProperty("value", getText(cruisingLevelElem));
                    cruisingLevel.addProperty("uom", getAttribute(cruisingLevelElem, "uom"));
                    routeInformation.add("cruisingLevel", cruisingLevel);
                }

                Element cruisingSpeedElem = getElement(routeInformationElem, "fx:cruisingSpeed");
                if (cruisingSpeedElem != null) {
                    JsonObject cruisingSpeed = new JsonObject();
                    cruisingSpeed.addProperty("value", getText(cruisingSpeedElem));
                    cruisingSpeed.addProperty("uom", getAttribute(cruisingSpeedElem, "uom"));
                    routeInformation.add("cruisingSpeed", cruisingSpeed);
                }

                JsonObject estimatedElapsedTime = new JsonObject();
                NodeList estimatedElapsedTimeElems = getElements(routeInformationElem, "fx:estimatedElapsedTime");
                for (int i = 0; i < estimatedElapsedTimeElems.getLength(); i++) {
                    Element elapsedTimeElem = (Element) estimatedElapsedTimeElems.item(i);
                    String elapsedTime = getAttribute(elapsedTimeElem, "elapsedTime");
                    String region = getText(getElement(elapsedTimeElem, "fx:region"));
                    JsonObject elapsedTimeObj = new JsonObject();
                    elapsedTimeObj.addProperty("elapsedTime", elapsedTime);
                    elapsedTimeObj.addProperty("region", region != null ? region : "unknown");
                    estimatedElapsedTime.add(elapsedTime, elapsedTimeObj);
                }
                routeInformation.add("estimatedElapsedTime", estimatedElapsedTime);
            }
            filed.add("routeInformation", routeInformation);

            JsonObject filedElements = new JsonObject();
            NodeList filedElementElems = getElements(filedElem, "fx:element");
            for (int i = 0; i < filedElementElems.getLength(); i++) {
                Element element = (Element) filedElementElems.item(i);
                String seqNum = getAttribute(element, "seqNum");
                JsonObject elementObj = new JsonObject();

                String routeDesignator = getText(getElement(element, "fx:routeDesignator"));
                if (routeDesignator != null) {
                    elementObj.addProperty("routeDesignator", routeDesignator);
                }

                String routePoint = getAttribute(getElement(element, "fx:routePoint"), "designator");
                if (routePoint != null) {
                    elementObj.addProperty("routePoint", routePoint);
                }

                String standardInstrumentArrival = getText(getElement(element, "fx:routeDesignatorToNextElement/fx:standardInstrumentArrival"));
                if (standardInstrumentArrival != null) {
                    elementObj.addProperty("standardInstrumentArrival", standardInstrumentArrival);
                }

                NodeList routeChanges = getElements(element, "fx:routeChange");
                for (int j = 0; j < routeChanges.getLength(); j++) {
                    Element routeChangeElem = (Element) routeChanges.item(j);

                    Element speedElem = getElement(routeChangeElem, "fx:speed");
                    if (speedElem != null) {
                        JsonObject speedChange = new JsonObject();
                        speedChange.addProperty("value", getText(speedElem));
                        speedChange.addProperty("uom", getAttribute(speedElem, "uom"));
                        elementObj.add("speedChange", speedChange);
                    }

                    Element levelElem = getElement(routeChangeElem, "fx:level/fb:flightLevel");
                    if (levelElem != null) {
                        JsonObject flightLevelChange = new JsonObject();
                        flightLevelChange.addProperty("value", getText(levelElem));
                        flightLevelChange.addProperty("uom", getAttribute(levelElem, "uom"));
                        elementObj.add("flightLevelChange", flightLevelChange);
                    }
                }

                if (elementObj.size() > 0) {
                    filedElements.add(seqNum, elementObj);
                }
            }
            filed.add("element", filedElements);
            flight.add("filed", filed);
        }

        return flight.toString();
    }

    // Helper functions
    private static String getAttribute(Element element, String attribute) {
        return element != null ? element.getAttribute(attribute) : null;
    }

    private static String getText(Element element) {
        return element != null ? element.getTextContent() : null;
    }

    private static String getTextContent(Element parent, String tagName) {
        Element element = getElement(parent, tagName);
        return getText(element);
    }

    private static Element getElement(Element parent, String tagName) {
        if (parent == null) return null;
        String[] parts = tagName.split(":");
        return (Element) parent.getElementsByTagNameNS(NAMESPACE.get(parts[0]), parts[1]).item(0);
    }

    private static NodeList getElements(Element parent, String tagName) {
        if (parent == null) return null;
        String[] parts = tagName.split(":");
        return parent.getElementsByTagNameNS(NAMESPACE.get(parts[0]), parts[1]);
    }
}
