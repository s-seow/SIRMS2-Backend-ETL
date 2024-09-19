package DynamoDB_ETL.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class METReport_DataConverter {

    public static String convertMETDataToJson(String id, String metarData) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode metReportJson = mapper.createObjectNode();

            // Add 'id' and 'datetime' to the JSON structure
            metReportJson.put("id", id);

            // 1. Split the report into meaningful parts by keywords
            String[] sections = splitByKeywords(metarData);

            // 2. Extract each section's respective data
            extractBasicReportData(sections[0], metReportJson);
            extractRunwayWindData(sections[1], metReportJson);
            extractRunwayVisibilityData(sections[2], metReportJson);
            extractCloudCoverAndTempData(sections[3], metReportJson);
            extractDewPointData(sections[4], metReportJson);
            extractPressureData(sections[5], metReportJson);
            extractTrendData(sections[6], metReportJson);

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metReportJson);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Find sections using keywords as boundaries
    private static String[] splitByKeywords(String metarData) {
        String[] sections = new String[7];

        // Basic Report data - ends at "WIND"
        sections[0] = metarData.split("WIND")[0];

        // Wind data - starts at "WIND", ends at "VIS"
        if (metarData.contains("WIND")) {
            sections[1] = metarData.split("WIND")[1].split("VIS")[0];
        } else {
            sections[1] = "";
        }

        // Visibility data - starts at "VIS", ends at "CLD"
        if (metarData.contains("VIS")) {
            sections[2] = metarData.split("VIS")[1].split("CLD")[0];
        } else {
            sections[2] = "";
        }

        // Cloud and Temperature data - starts at "CLD", ends at "DP"
        if (metarData.contains("CLD")) {
            sections[3] = metarData.split("CLD")[1].split("DP")[0];
        } else {
            sections[3] = "";
        }

        // Dew point data - starts at "DP", ends at "QNH"
        if (metarData.contains("DP")) {
            sections[4] = metarData.split("DP")[1].split("QNH")[0];
        } else {
            sections[4] = "";
        }

        // Pressure data - starts at "QNH", ends at "TREND" (if present)
        if (metarData.contains("QNH")) {
            if (metarData.contains("TREND")) {
                sections[5] = metarData.split("QNH")[1].split("TREND")[0];
            } else {
                sections[5] = metarData.split("QNH")[1];
            }
        } else {
            sections[5] = "";
        }

        // Trend data - starts at "TREND"
        if (metarData.contains("TREND")) {
            sections[6] = metarData.split("TREND")[1];
        } else {
            sections[6] = "";
        }

        return sections;
    }


    private static void extractBasicReportData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing Basic Report Details: " + line); ERROR CHECK

        String[] firstLineParts = line.trim().split("\\s+");
        if (firstLineParts.length >= 3) {
            String reportType = firstLineParts[0];
            String station = firstLineParts[1];
            String reportDateTime = firstLineParts[2];

            metReportJson.put("reportType", reportType);
            metReportJson.put("station", station);
            metReportJson.put("dateTime", reportDateTime);
        } else {
            throw new IllegalArgumentException("Basic report details missing or malformed.");
        }
    }


    private static void extractRunwayWindData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing WIND: " + line); ERROR CHECK
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode windDataNode = mapper.createObjectNode();

        // Split wind data into runway-specific sections
        String[] runwayWindParts = line.split("RWY");

        for (String runwayWindPart : runwayWindParts) {
            if (!runwayWindPart.trim().isEmpty()) {
                String[] parts = runwayWindPart.trim().split("\\s+");
                if (parts.length >= 3) {
                    String runway = "RWY " + parts[0]; // Extract the runway (e.g., 02L, 02C)
                    ObjectNode windDetails = mapper.createObjectNode();

                    String tdz = "", mid = "", end = "";
                    String tdzVRB = "", midVRB = "", endVRB = "";

                    // Iterate over parts to extract TDZ, MID, END and their corresponding VRB
                    for (int i = 1; i < parts.length; i++) {
                        if (parts[i].equals("TDZ")) {
                            tdz = parts[i + 1]; // Extract the TDZ value
                        } else if (parts[i].equals("MID")) {
                            mid = parts[i + 1]; // Extract the MID value
                        } else if (parts[i].equals("END")) {
                            end = parts[i + 1]; // Extract the END value
                        } else if (parts[i].equals("VRB")) {
                            // Dynamically assign VRB to the nearest section (TDZ, MID, END)
                            if (i > 1 && parts[i - 2].equals("TDZ")) {
                                tdzVRB = "VRB BTN " + parts[i + 2] + " AND " + parts[i + 4];
                            } else if (i > 1 && parts[i - 2].equals("MID")) {
                                midVRB = "VRB BTN " + parts[i + 2] + " AND " + parts[i + 4];
                            } else if (i > 1 && parts[i - 2].equals("END")) {
                                endVRB = "VRB BTN " + parts[i + 2] + " AND " + parts[i + 4];
                            }
                        }
                    }

                    // Add TDZ, MID, END, and their corresponding VRB BTN if available
                    if (!tdz.isEmpty()) {
                        windDetails.put("TDZ", tdz);
                    }
                    if (!tdzVRB.isEmpty()) {
                        windDetails.put("TDZ_VariableWind", tdzVRB);
                    }
                    if (!mid.isEmpty()) {
                        windDetails.put("MID", mid);
                    }
                    if (!midVRB.isEmpty()) {
                        windDetails.put("MID_VariableWind", midVRB);
                    }
                    if (!end.isEmpty()) {
                        windDetails.put("END", end);
                    }
                    if (!endVRB.isEmpty()) {
                        windDetails.put("END_VariableWind", endVRB);
                    }

                    windDataNode.set(runway, windDetails);
                }
            }
        }

        metReportJson.set("wind", windDataNode);
    }


    private static void extractRunwayVisibilityData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing VIS: " + line); ERROR CHECK
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode visData = mapper.createObjectNode();

        String[] runwayParts = line.split("RWY");

        // Iterate over each runway section
        for (String runwayPart : runwayParts) {
            if (!runwayPart.trim().isEmpty()) {
                String[] parts = runwayPart.trim().split("\\s+");

                // The first part should be the runway identifier, e.g., '02L', '02C', '02R'
                String runway = "RWY " + parts[0];

                ObjectNode visibilityDetails = mapper.createObjectNode();

                String tdz = "", mid = "", end = "";

                for (int i = 1; i < parts.length; i++) {
                    if (parts[i].equals("TDZ")) {
                        tdz = parts[i+1]; // Extract the TDZ value
                    } else if (parts[i].equals("MID")) {
                        mid = parts[i+1]; // Extract the MID value
                    } else if (parts[i].equals("END")) {
                        end = parts[i+1]; // Extract the END value
                    }
                }

                if (!tdz.isEmpty()) {
                    visibilityDetails.put("TDZ", tdz);
                }
                if (!mid.isEmpty()) {
                    visibilityDetails.put("MID", mid);
                }
                if (!end.isEmpty()) {
                    visibilityDetails.put("END", end);
                }

                visData.set(runway, visibilityDetails);
            }
        }

        metReportJson.set("visibility", visData);
    }


    private static void extractCloudCoverAndTempData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing CC and Temp: " + line); ERROR CHECK
        String[] cloudParts = line.split("\\s+");
        StringBuilder cloudCoverBuilder = new StringBuilder();

        for (int i = 1; i < cloudParts.length; i++) {
            // Look for the temperature part (only starts with 'T' followed by a number)
            if (cloudParts[i].matches("^T\\d+$")) {
                String temperature = cloudParts[i].replace("T", "") + "°C";
                metReportJson.put("temperature", temperature);
            } else {
                cloudCoverBuilder.append(cloudParts[i]).append(" ");
            }
        }

        String cloudCover = cloudCoverBuilder.toString().trim();
        metReportJson.put("cloudCover", cloudCover);
    }


    private static void extractDewPointData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing DP: " + line); ERROR CHECK
        String[] dewPointParts = line.split("\\s+");

        String dewPoint = dewPointParts[0].replace("DP", "") + "°C";
        metReportJson.put("dewPoint", dewPoint);
    }


    private static void extractPressureData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing Pressure: " + line); ERROR CHECK
        String[] tempPressureParts = line.split("\\s+");

        if (tempPressureParts.length >= 2) {
            String pressure = tempPressureParts[1].replace("QNH", "").replace("HPA", " hPa");

            metReportJson.put("pressure", pressure);
        }
    }


    private static void extractTrendData(String line, ObjectNode metReportJson) {
        //System.out.println("Processing TREND: " + line); ERROR CHECK
        String[] trendParts = line.split("\\s+");
        if (trendParts.length >= 2) {
            metReportJson.put("trend", trendParts[1]);
        }
    }
}
