package cs523.SparkKafkaConsumer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

import cs523.SparkKafkaConsumer.WeatherData.Datum;


public class HBaseDBManager {
	private Configuration hbaseConfig;
	private int rowkeyAnalysis = 0;
	int count = 0;

	// Sanjib Vai
	private final String TABLE_WEATHER_NAME = "tbl_weather";
	private final String COLUMN_CITY = "city";
	private final String COLUMN_DATA = "data";
	private final String SUB_COLUMN_NAME = "name";
	private final String SUB_COLUMN_COUNTRY = "country";
	private final String SUB_COLUMN_LON = "lon";
	private final String SUB_COLUMN_LAT = "lat";
	private final String SUB_COLUMN_YEAR = "year_val";
	private final String SUB_COLUMN_MONTH = "month_val";
	private final String SUB_COLUMN_DAY = "day_val";
	private final String SUB_COLUMN_MIN = "min_val";
	private final String SUB_COLUMN_MAX = "max_val";
	private final String SUB_COLUMN_DAY_TEMP = "daytemp";

	public HBaseDBManager() throws IOException {
		this.hbaseConfig = HBaseConfiguration.create();
	}

	public void writeWeatherData( JavaRDD<WeatherData> rdd)
			throws IOException {

		try (Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_WEATHER_NAME));
			table.addFamily(new HColumnDescriptor(COLUMN_CITY)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(COLUMN_DATA));
			if (!admin.tableExists(table.getTableName())) {
				admin.createTable(table);
			}

			Table tbl = connection.getTable(TableName
					.valueOf(TABLE_WEATHER_NAME));
			int count = 0;
			for (WeatherData wd : rdd.collect()) {
				
				for (Datum wd_datum : wd.getData()) {

					Put put = new Put(Bytes.toBytes(String
							.valueOf(++this.rowkeyAnalysis)));
					put.addColumn(Bytes.toBytes(COLUMN_CITY),
							Bytes.toBytes(SUB_COLUMN_NAME),
							Bytes.toBytes(wd.getCity().getName()));
					put.addColumn(Bytes.toBytes(COLUMN_CITY),
							Bytes.toBytes(SUB_COLUMN_COUNTRY),
							Bytes.toBytes(wd.getCity().getCountry()));
					put.addColumn(
							Bytes.toBytes(COLUMN_CITY),
							Bytes.toBytes(SUB_COLUMN_LON),
							Bytes.toBytes(wd.getCity().getCoord().getLon()
									.toString()));
					put.addColumn(
							Bytes.toBytes(COLUMN_CITY),
							Bytes.toBytes(SUB_COLUMN_LAT),
							Bytes.toBytes(wd.getCity().getCoord().getLat()
									.toString()));

					LocalDateTime ldt = Utils.getTimeStampToDate(wd_datum.getDt());
					LocalDate dt = ldt.toLocalDate();
					
					LocalTime localTime = ldt.toLocalTime();

					put.addColumn(Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_YEAR),
							Bytes.toBytes(Integer.toString(dt.getYear())));
					put.addColumn(Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_MONTH),
							Bytes.toBytes(Integer.toString(dt.getMonthValue())));
					put.addColumn(Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_DAY),
							Bytes.toBytes(Integer.toString(dt.getDayOfMonth())));
					
					
					wd_datum.getTemp().setMin(wd_datum.getTemp().getMin()/10);
                    wd_datum.getTemp().setMax(wd_datum.getTemp().getMax()/10);
                    wd_datum.getTemp().setDay(wd_datum.getTemp().getDay()/10);

					put.addColumn(
							Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_MIN),
							Bytes.toBytes(wd_datum.getTemp().getMin()
									.toString()));
					put.addColumn(
							Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_MAX),
							Bytes.toBytes(wd_datum.getTemp().getMax()
									.toString()));
					put.addColumn(
							Bytes.toBytes(COLUMN_DATA),
							Bytes.toBytes(SUB_COLUMN_DAY_TEMP),
							Bytes.toBytes(wd_datum.getTemp().getDay()
									.toString()));

					System.out.println("max " + wd_datum.getTemp().getMax()
							+ " Min " + wd_datum.getTemp().getMin() + " Day "
							+ wd_datum.getTemp().getDay());

					tbl.put(put);
					count++;
				}
			}
			tbl.close();

			if (count > 0)
				System.out.println("tbl_weather written rows count:" + count);
		}
	}
}
