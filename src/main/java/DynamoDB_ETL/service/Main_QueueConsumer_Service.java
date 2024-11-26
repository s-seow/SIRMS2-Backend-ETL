package DynamoDB_ETL.service;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jms.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;

@Service
@PropertySource("classpath:solace.properties")
public class Main_QueueConsumer_Service {

    private final String QUEUE_NAME = "q/sirms-prototype";

    @Value("${solace.host}")
    private String solaceHost;

    @Value("${solace.username}")
    private String solaceUsername;

    @Value("${solace.password}")
    private String solacePassword;

    private final FIXM_DataLoader_Service FIXM_DataLoader_Service;
    private final IWXXM_DataLoader_Service IWXXM_DataLoader_Service;
    private final METReport_DataLoader_Service METReport_DataLoader_Service;


    public Main_QueueConsumer_Service(FIXM_DataLoader_Service FIXM_DataLoader_Service,
                                      IWXXM_DataLoader_Service IWXXM_DataLoader_Service,
                                      METReport_DataLoader_Service METReport_DataLoader_Service) {
        this.FIXM_DataLoader_Service = FIXM_DataLoader_Service;
        this.IWXXM_DataLoader_Service = IWXXM_DataLoader_Service;
        this.METReport_DataLoader_Service = METReport_DataLoader_Service;
    }


    @PostConstruct
    public void start() throws Exception {
        System.out.printf("QueueConsumer is connecting to Solace router %s...%n", solaceHost);

        // String connectionURI = "amqps://broker.swimapisg.info:5675?transport.trustAll=true&transport.verifyHost=false";

        ConnectionFactory connectionFactory = new JmsConnectionFactory(solaceUsername, solacePassword, solaceHost);

        try (JMSContext context = connectionFactory.createContext()) {

            System.out.printf("Connected with username '%s'.%n", solaceUsername);

            Queue queue = context.createQueue(QUEUE_NAME);

            System.out.println("Awaiting message...");
            System.out.println();

            JMSConsumer consumer = context.createConsumer(queue);

            while (true) {
                Message message = consumer.receive();

                if (message instanceof TextMessage) {
                    processTextMessage((TextMessage) message);
                }

                else if (message instanceof JmsBytesMessage) {
                    processBytesMessage((BytesMessage) message);
                }

                else {
                    System.out.printf("Message type received: '%s'%n", message.getJMSType());
                    System.out.printf(" %s_%s_%s_%s%n", message.getJMSDestination(), message.getJMSMessageID(), message.getJMSReplyTo(), message.getStringProperty("JMSXDeliveryCount"));
                    System.out.println();
                }
            }
        }
    }


    private void processBytesMessage(BytesMessage message) throws JMSException {
        byte[] byteData = new byte[(int) message.getBodyLength()];
        message.readBytes(byteData);
        message.reset();
        String stringMessage = new String(byteData);
        System.out.printf("BytesMessage received: '%s'%n", stringMessage);

        String formattedTimestamp = convertJMSTimestamp(message);
        String jmsDestination = message.getJMSDestination().toString();

        //System.out.printf(" logTimestamp: %s%n", formattedTimestamp);
        System.out.printf(" Topic:%s%n", jmsDestination);
        System.out.println("BytesMessage received. Skipping...");

        System.out.println();
    }


    public static String convertJMSTimestamp(Message message) throws JMSException {

        long jmsTimestamp = message.getJMSTimestamp();
        LocalDateTime logTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(jmsTimestamp), ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        return logTimestamp.format(formatter);
    }


    private void processTextMessage(TextMessage message) throws JMSException {

        String messageContent = message.getText();
        String formattedTimestamp = convertJMSTimestamp(message);
        String jmsMessageID = message.getJMSMessageID();
        String jmsDestination = message.getJMSDestination().toString();

        Enumeration<?> propertyNames = message.getPropertyNames();
        StringBuilder propertiesString = new StringBuilder("Message Content: [");

        while (propertyNames.hasMoreElements()) {
            String propertyName = (String) propertyNames.nextElement();
            String propertyValue = message.getStringProperty(propertyName);
            propertiesString.append(propertyName)
                    .append(":")
                    .append(propertyValue);
            if (propertyNames.hasMoreElements()) {
                propertiesString.append(", ");
            }
        }
        propertiesString.append("]");

        // 1. Print Properties string first
        System.out.println(propertiesString);

        // 2. Print the main text body next
        System.out.printf("TextMessage received: '%s'%n", messageContent);

        // 3. Print additional information: logTimestamp (sort key), messageID, messageDestination
        System.out.printf(" logTimestamp: %s%n", formattedTimestamp);
        System.out.printf(" %s%n", jmsMessageID);
        System.out.printf(" Topic:%s%n", jmsDestination);

        // To determine type of data
        if (jmsDestination.contains("fixm")) {
            System.out.println("Processing FIXM data...");
            FIXM_DataLoader_Service.processMessageContent(messageContent, formattedTimestamp, jmsMessageID, jmsDestination);
        }
        else if (jmsDestination.contains("iwxxm")) {
            System.out.println("Processing IWXXM data...");
            IWXXM_DataLoader_Service.processMessageContent(messageContent, formattedTimestamp, jmsMessageID, jmsDestination);
        }
        else if (jmsDestination.contains("met-report")) {
            System.out.println("Processing MET Report data...");
            METReport_DataLoader_Service.processMessageContent(messageContent, formattedTimestamp, jmsMessageID, jmsDestination);
        }
        else {
            System.out.println("Skipping...");
            System.out.println();
        }
    }
}
