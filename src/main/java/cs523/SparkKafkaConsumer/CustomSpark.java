package cs523.SparkKafkaConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.gson.Gson;

import scala.Tuple2;

public class CustomSpark {
	static int counter = 1;
	private JavaSparkContext sc;
	private HBaseDBManager db;
	private static Gson gson;

	public CustomSpark() throws IOException {
		this.sc = new JavaSparkContext("local[*]", "MySpark", new SparkConf());
		this.db = new HBaseDBManager();
		gson = new Gson();
	}

	public void Process(String key, List<String> l) throws IOException {
		
		JavaRDD<String> list = this.sc.parallelize(l).filter(
				line -> !line.isEmpty());
		JavaPairRDD<String, WeatherData> weatherResults = list
				.mapToPair(new MyPairFunctionForWeather(key))
//				.filter(f -> {
//					//filter by year
//					LocalDate dt = Utils.getTimeStampToDate(f._2.getTime());
//					
//					if (dt.getYear() == 2017)
//						return true;
//					else{
//						//System.out.println(dt.getYear());
//						return false;
//					}
//						
//				})
//				.filter(f->{
//					// Filter by city
//					
//					if(f._2.getCity().getName().toLowerCase().equals("miranpur") ){
//						return true;
//					}else{
//						//System.out.println("Country code filtered out is - "+f._2.getCity().getCountry());
//						return false;
//					}
//				})
//				.filter(f->{
//					// Filter by country
//					
//					if(f._2.getCity().getCountry().toLowerCase().equals("in") ){
//						return true;
//					}else{
//						//System.out.println("Country code filtered out is - "+f._2.getCity().getCountry());
//						return false;
//					}
//				})
				.sortByKey()
				;

		this.db.writeWeatherData(weatherResults.values());
	}

	static class MyPairFunctionForWeather implements
			PairFunction<String, String, WeatherData>, Serializable {
		private String key;

		public MyPairFunctionForWeather(String k) {
			this.key = k;
		}

		private static final long serialVersionUID = 12312L;

		@Override
		public Tuple2<String, WeatherData> call(String _line) throws Exception {
			WeatherData wd = gson.fromJson(_line, WeatherData.class);
			return new Tuple2<String, WeatherData>(this.key, wd);
		}
	}
}
