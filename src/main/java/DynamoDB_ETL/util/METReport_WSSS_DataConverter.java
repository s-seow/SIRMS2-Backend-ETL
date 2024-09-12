package DynamoDB_ETL.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class METReport_WSSS_DataConverter {

    public static String convertWSSSMETDataToJson(String metarData) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode metReportJson = mapper.createObjectNode();

            // Split the METAR data by newlines
            String[] lines = metarData.split("\\n+");

            // Extract and add metadata
            String[] firstLineParts = lines[0].split("\\s+");
            String reportType = firstLineParts[0];
            String station = firstLineParts[1];
            String dateTime = firstLineParts[2];

            metReportJson.put("reportType", reportType);
            metReportJson.put("station", station);
            metReportJson.put("dateTime", dateTime);

            // Extract wind, visibility, cloud, temperature, pressure, trend, etc.
            extractRunwayData(lines, metReportJson);
            extractCloudCover(lines, metReportJson);
            extractTemperature(lines, metReportJson);
            extractPressure(lines, metReportJson);
            extractTrend(lines, metReportJson);

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

        for (String line : lines) {
            if (line.contains("RWY")) {
                String[] runwayParts = line.split("\\s+");
                String runway = runwayParts[1];

                ObjectNode runwayDetails = mapper.createObjectNode();
                ObjectNode windDetails = mapper.createObjectNode();

                windDetails.put("TDZ", runwayParts[3] + " " + runwayParts[4]);
                windDetails.put("MID", runwayParts[6] + " " + runwayParts[7]);
                windDetails.put("END", runwayParts[9] + " " + runwayParts[10]);

                // Add variable wind details for each section (if exists)
                ObjectNode variableWindDetails = mapper.createObjectNode();
                addVariableWind(line, variableWindDetails, "TDZ");
                addVariableWind(line, variableWindDetails, "MID");
                addVariableWind(line, variableWindDetails, "END");

                if (!variableWindDetails.isEmpty()) {
                    runwayDetails.set("Variable Wind", variableWindDetails);
                }

                runwayDetails.set("Wind", windDetails);

                // Extract and add visibility details for each runway section
                String visibility = extractVisibility(line);
                if (visibility != null) {
                    runwayDetails.put("Visibility", visibility);
                }

                runwayData.set(runway, runwayDetails);
            }
        }

        metReportJson.set("runways", runwayData);
    }

    private static void addVariableWind(String line, ObjectNode variableWindDetails, String section) {
        if (line.contains("VRB") && line.contains(section)) {
            String[] parts = line.split("VRB BTN");
            if (parts.length > 1) {
                String[] windRange = parts[1].trim().split("AND");
                if (windRange.length == 2) {
                    variableWindDetails.put(section, windRange[0].trim() + " - " + windRange[1].trim());
                }
            }
        }
    }

    private static String extractVisibility(String line) {
        if (line.contains("VIS")) {
            // Example: "VIS RWY 02L TDZ 10KM MID 10KM END 10KM"
            String[] visibilityParts = line.split("VIS");
            if (visibilityParts.length > 1) {
                String[] visDetails = visibilityParts[1].split("\\s+");
                return "TDZ " + visDetails[2] + " MID " + visDetails[4] + " END " + visDetails[6];
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

    private static void extractTrend(String[] lines, ObjectNode metReportJson) {
        for (String line : lines) {
            if (line.contains("TREND")) {
                String[] trendParts = line.split("\\s+");
                metReportJson.put("trend", trendParts[1]);
            }
        }
    }
}
