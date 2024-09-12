package DynamoDB_ETL.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class METReport_WSSL_DataConverter {

    public static String convertWSSLMETDataToJson(String metarData) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode metReportJson = mapper.createObjectNode();

            // Split the METAR data by newlines
            String[] lines = metarData.split("\\n+");

            // Extract and add metadata
            String[] firstLineParts = lines[0].split("\\s+");
            String reportType = firstLineParts[0]; // MRXX01
            String station = firstLineParts[1]; // WSSL
            String dateTime = firstLineParts[2]; // 050600Z

            metReportJson.put("reportType", reportType);
            metReportJson.put("station", station);
            metReportJson.put("dateTime", dateTime);

            // Extract wind, visibility, cloud, temperature, and pressure data
            extractRunwayData(lines, metReportJson);
            extractCloudCover(lines, metReportJson);
            extractTemperature(lines, metReportJson);
            extractPressure(lines, metReportJson);

            // Return the JSON as a string
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metReportJson);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void extractRunwayData(String[] lines, ObjectNode metReportJson) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode runwayData = mapper.createObjectNode();

        // Extract the wind, visibility, and other runway-specific information
        for (String line : lines) {
            if (line.contains("RWY 03")) {
                String[] runwayParts = line.split("\\s+");

                // For RWY 03
                String runway = runwayParts[1];
                ObjectNode runwayDetails = mapper.createObjectNode();

                // Extract wind data for TDZ and END
                ObjectNode windDetails = mapper.createObjectNode();
                windDetails.put("TDZ", runwayParts[3] + " " + runwayParts[4]);
                windDetails.put("END", runwayParts[6] + " " + runwayParts[7]);

                // Extract variable wind data for TDZ and END
                ObjectNode variableWindDetails = mapper.createObjectNode();
                variableWindDetails.put("TDZ", runwayParts[5] + " - " + runwayParts[9]);
                variableWindDetails.put("END", runwayParts[8] + " - " + runwayParts[11]);

                runwayDetails.set("Wind", windDetails);
                runwayDetails.set("Variable Wind", variableWindDetails);

                // Extract visibility
                String visibility = extractVisibility(line);
                if (visibility != null) {
                    runwayDetails.put("Visibility", visibility);
                }

                runwayData.set(runway, runwayDetails);
            }
        }

        metReportJson.set("runways", runwayData);
    }

    private static String extractVisibility(String line) {
        if (line.contains("VIS")) {
            String[] visibilityParts = line.split("VIS");
            if (visibilityParts.length > 1) {
                String[] visDetails = visibilityParts[1].split("\\s+");
                return "TDZ " + visDetails[2] + " END " + visDetails[4];
            }
        }
        return null;
    }

    private static void extractCloudCover(String[] lines, ObjectNode metReportJson) {
        for (String line : lines) {
            if (line.contains("CLD")) {
                String[] cloudParts = line.split("\\s+");
                String cloudCover = cloudParts[1] + " at " + cloudParts[2];
                metReportJson.put("cloudCover", cloudCover);
            }
        }
    }

    private static void extractTemperature(String[] lines, ObjectNode metReportJson) {
        for (String line : lines) {
            if (line.contains("T")) {
                String[] tempParts = line.split("\\s+");
                String temperature = tempParts[0].replace("T", "") + "°C";
                String dewPoint = tempParts[1].replace("DP", "") + "°C";
                metReportJson.put("temperature", temperature);
                metReportJson.put("dewPoint", dewPoint);
            }
        }
    }

    private static void extractPressure(String[] lines, ObjectNode metReportJson) {
        for (String line : lines) {
            if (line.contains("QNH")) {
                String[] pressureParts = line.split("\\s+");
                String pressure = pressureParts[1].replace("HPA", " hPa");
                metReportJson.put("pressure", pressure);
            }
        }
    }
}