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

            // 1. Split the report into parts by keywords
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
            // String reportType = firstLineParts[0];
            String station = firstLineParts[1];
            String reportDateTime = firstLineParts[2];

            // metReportJson.put("reportType", reportType);
            metReportJson.put("aerodrome", station);
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
                        if (i + 1 < parts.length) {
                            if (parts[i].equals("TDZ")) {
                                tdz = parts[i+1]; // Extract the TDZ value
                            } else if (parts[i].equals("MID")) {
                                mid = parts[i+1]; // Extract the MID value
                            } else if (parts[i].equals("END")) {
                                end = parts[i+1]; // Extract the END value
                            }
                        }

                        // few cases possible:
                        // 1. TDZ 120/5KT (normal)
                        // 2. TDZ 120/5KT VRB BTN 100/ AND 130/ parts[i-2]
                        // 3. TDZ VRB3KT parts[i-1]
                        // 4. TDZ VRB BTN 100/ AND 130/2KT parts[i-1]

                        if (parts[i].contains("VRB")) {
                            // case 4: TDZ VRB BTN 100/ AND 130/2KT
                            if (i + 4 < parts.length && parts[i + 1].equals("BTN")) {
                                if (i > 1 && parts[i-1].equals("TDZ")) {
                                    tdzVRB = parts[i+2] + " AND " + parts[i+4];
                                    // return 100/ AND 130/2KT to parser (.contains("AND") should be second)
                                }
                                else if (i > 1 && parts[i-1].equals("MID")) {
                                    midVRB = parts[i+2] + " AND " + parts[i+4];
                                }
                                else if (i > 1 && parts[i-1].equals("END")) {
                                    endVRB = parts[i+2] + " AND " + parts[i+4];
                                }
                            }

                            // case 3: TDZ VRB3KT
                            else if (i > 1 && parts[i-1].equals("TDZ")) {
                                tdzVRB = parts[i];
                                // return VRB3KT to parser (.contains("KT") should be last)
                            } else if (i > 1 && parts[i-1].equals("MID")) {
                                midVRB = parts[i];
                            } else if (i > 1 && parts[i-1].equals("END")) {
                                endVRB = parts[i];
                            }

                            // case 2: TDZ 120/5KT VRB BTN 100/ AND 130/
                            else if (i > 1 && parts[i-2].equals("TDZ")) {
                                tdzVRB = "VRB BTN " + parts[i+2] + " AND " + parts[i+4];
                                // return VRB BTN 100/ AND 130/ to parser (.contains("BTN") should be first)
                            } else if (i > 1 && parts[i-2].equals("MID")) {
                                midVRB = "VRB BTN " + parts[i+2] + " AND " + parts[i+4];
                            } else if (i > 1 && parts[i-2].equals("END")) {
                                endVRB = "VRB BTN " + parts[i+2] + " AND " + parts[i+4];
                            }
                        }
                    }

                    if (!tdz.isEmpty()) {
                        windDetails.set("TDZ", parseWindValue(tdz));
                    }
                    if (!tdzVRB.isEmpty()) {
                        windDetails.set("TDZ_VariableWind", parseVariableWindValue(tdzVRB));
                    }
                    if (!mid.isEmpty()) {
                        windDetails.set("MID", parseWindValue(mid));
                    }
                    if (!midVRB.isEmpty()) {
                        windDetails.set("MID_VariableWind", parseVariableWindValue(midVRB));
                    }
                    if (!end.isEmpty()) {
                        windDetails.set("END", parseWindValue(end));
                    }
                    if (!endVRB.isEmpty()) {
                        windDetails.set("END_VariableWind", parseVariableWindValue(endVRB));
                    }

                    windDataNode.set(runway, windDetails);
                }
            }
        }

        metReportJson.set("wind", windDataNode);
    }


    private static ObjectNode parseWindValue(String windValue) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode windNode = mapper.createObjectNode();

        if (windValue.contains("/")) {
            String[] parts = windValue.split("/");
            if (parts.length == 2) {
                String angle = parts[0];
                String speed = parts[1];

                String numberPortion = speed.replaceAll("[^0-9]", "");
                String unitPortion = speed.replaceAll("[0-9]", "");

                windNode.put("windDirection", angle);
                windNode.put("windSpeed", numberPortion);
                windNode.put("windSpeedUom", unitPortion);
            }
        }

        return windNode;
    }


    private static ObjectNode parseVariableWindValue(String vrbValue) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode vrbNode = mapper.createObjectNode();

        if (vrbValue.contains("BTN")) {
            // return VRB BTN 100/ AND 130/ to parser (.contains("BTN") should be first)
            String[] parts = vrbValue.replace("VRB BTN", "").split("AND");
            if (parts.length == 2) {
                String direction1 = parts[0].split("/")[0].trim();
                String direction2 = parts[1].split("/")[0].trim();

                vrbNode.put("variableWindDirection", direction1 + "-" + direction2);
            }
        }

        else if (vrbValue.contains("AND")) {
            // return 100/ AND 130/2KT to parser (.contains("AND") should be second)
            String[] vrbParts = vrbValue.split("AND");
            if (vrbParts.length == 2) {
                String direction1 = vrbParts[0].split("/")[0].trim();
                String direction2 = vrbParts[1].split("/")[0].trim();
                String speed = vrbParts[1].split("/")[1].trim();

                String numberPortion = speed.replaceAll("[^0-9]", "");
                String unitPortion = speed.replaceAll("[0-9]", "");

                vrbNode.put("variableWindDirection", direction1 + "-" + direction2);
                vrbNode.put("variableWindSpeed", numberPortion);
                vrbNode.put("variableWindSpeedUom", unitPortion);
            }
        }

        else if (vrbValue.contains("KT")) {
            // return VRB3KT to parser (.contains("KT") should be last)
            String speed = vrbValue.replace("VRB", "").trim();

            String numberPortion = speed.replaceAll("[^0-9]", "");
            String unitPortion = speed.replaceAll("[0-9]", "");

            vrbNode.put("variableWindSpeed", numberPortion);
            vrbNode.put("variableWindSpeedUom", unitPortion);
        }

        return vrbNode;
    }


    private static void extractRunwayVisibilityData(String line, ObjectNode metReportJson) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode visData = mapper.createObjectNode();

        String[] runwayParts = line.split("RWY");

        for (String runwayPart : runwayParts) {
            if (!runwayPart.trim().isEmpty()) {
                String[] parts = runwayPart.trim().split("\\s+");

                String runway = "RWY " + parts[0];

                ObjectNode visibilityDetails = mapper.createObjectNode();

                String tdz = "", mid = "", end = "";

                for (int i = 1; i < parts.length; i++) {
                    if (parts[i].equals("TDZ")) {
                        tdz = parts[i + 1]; // Extract the TDZ value
                    } else if (parts[i].equals("MID")) {
                        mid = parts[i + 1]; // Extract the MID value
                    } else if (parts[i].equals("END")) {
                        end = parts[i + 1]; // Extract the END value
                    }
                }

                // Split visibility values into number and unit
                if (!tdz.isEmpty()) {
                    ObjectNode tdzVisibility = parseVisibilityValue(tdz);
                    visibilityDetails.set("TDZ", tdzVisibility);
                }
                if (!mid.isEmpty()) {
                    ObjectNode midVisibility = parseVisibilityValue(mid);
                    visibilityDetails.set("MID", midVisibility);
                }
                if (!end.isEmpty()) {
                    ObjectNode endVisibility = parseVisibilityValue(end);
                    visibilityDetails.set("END", endVisibility);
                }

                visData.set(runway, visibilityDetails);
            }
        }

        metReportJson.set("visibility", visData);
    }

    private static ObjectNode parseVisibilityValue(String visibilityValue) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode visibilityNode = mapper.createObjectNode();

        String numberPortion = visibilityValue.replaceAll("[^0-9]", "");
        String unitPortion = visibilityValue.replaceAll("[0-9]", "");

        visibilityNode.put("visibility", numberPortion);
        visibilityNode.put("visibilityUom", unitPortion);

        return visibilityNode;
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
