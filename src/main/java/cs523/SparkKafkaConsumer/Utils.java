package cs523.SparkKafkaConsumer;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;



public class Utils {
	
	public static LocalDateTime getTimeStampToDate(long ts){
		// Convert the timestamp to a date object
		Timestamp timestamp = new Timestamp(ts*1000);
		LocalDateTime  ld = timestamp.toLocalDateTime();
		return ld;
	}

	

}
