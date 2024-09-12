package DynamoDB_ETL;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DynamoDbETLApplication {
	public static void main(String[] args) {
		SpringApplication.run(DynamoDbETLApplication.class, args);
	}
}
