package cs523.SparkKafkaConsumer;


import java.io.Serializable;
import java.util.List;



public class WeatherData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 115426423L;
	private City city;
	private Long time;
	private List<Datum> data = null;

	public City getCity() {
		return city;
	}

	public void setCity(City city) {
		this.city = city;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public List<Datum> getData() {
		return data;
	}

	public void setData(List<Datum> data) {
		this.data = data;
	}

	public class City implements Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 5024581630063175370L;
		private Long id;
		private String name;
		private String country;
		private Coord coord;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public Coord getCoord() {
			return coord;
		}

		public void setCoord(Coord coord) {
			this.coord = coord;
		}

		public class Coord implements Serializable{

			/**
			 * 
			 */
			private static final long serialVersionUID = -1056036164452259140L;
			private Double lon;
			private Double lat;

			public Double getLon() {
				return lon;
			}

			public void setLon(Double lon) {
				this.lon = lon;
			}

			public Double getLat() {
				return lat;
			}

			public void setLat(Double lat) {
				this.lat = lat;
			}

		}

	}

	public class Datum implements Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 6051480236824548750L;
		private Long dt;
		private Temp temp;
		private Double pressure;
		private Long humidity;
		private List<Weather> weather = null;
		private Double speed;
		private Long deg;
		private Long clouds;
		private Double uvi;
		private Double snow;
		private Double rain;

		public Long getDt() {
			return dt;
		}

		public void setDt(Long dt) {
			this.dt = dt;
		}

		public Temp getTemp() {
			return temp;
		}

		public void setTemp(Temp temp) {
			this.temp = temp;
		}

		public Double getPressure() {
			return pressure;
		}

		public void setPressure(Double pressure) {
			this.pressure = pressure;
		}

		public Long getHumidity() {
			return humidity;
		}

		public void setHumidity(Long humidity) {
			this.humidity = humidity;
		}

		public List<Weather> getWeather() {
			return weather;
		}

		public void setWeather(List<Weather> weather) {
			this.weather = weather;
		}

		public Double getSpeed() {
			return speed;
		}

		public void setSpeed(Double speed) {
			this.speed = speed;
		}

		public Long getDeg() {
			return deg;
		}

		public void setDeg(Long deg) {
			this.deg = deg;
		}

		public Long getClouds() {
			return clouds;
		}

		public void setClouds(Long clouds) {
			this.clouds = clouds;
		}

		public Double getUvi() {
			return uvi;
		}

		public void setUvi(Double uvi) {
			this.uvi = uvi;
		}

		public Double getSnow() {
			return snow;
		}

		public void setSnow(Double snow) {
			this.snow = snow;
		}

		public Double getRain() {
			return rain;
		}

		public void setRain(Double rain) {
			this.rain = rain;
		}

		public class Temp implements Serializable{

			/**
			 * 
			 */
			private static final long serialVersionUID = -4984536984274957789L;
			private Double day;
			private Double min;
			private Double max;
			private Double night;
			private Double eve;
			private Double morn;

			public Double getDay() {
				return day;
			}

			public void setDay(Double day) {
				this.day = day;
			}

			public Double getMin() {
				return min;
			}

			public void setMin(Double min) {
				this.min = min;
			}

			public Double getMax() {
				return max;
			}

			public void setMax(Double max) {
				this.max = max;
			}

			public Double getNight() {
				return night;
			}

			public void setNight(Double night) {
				this.night = night;
			}

			public Double getEve() {
				return eve;
			}

			public void setEve(Double eve) {
				this.eve = eve;
			}

			public Double getMorn() {
				return morn;
			}

			public void setMorn(Double morn) {
				this.morn = morn;
			}

		}

		
		public class Weather implements Serializable{

			/**
			 * 
			 */
			private static final long serialVersionUID = -2727896584598234367L;
			private Long id;
			private String main;
			private String description;
			private String icon;

			public Long getId() {
			return id;
			}

			public void setId(Long id) {
			this.id = id;
			}

			public String getMain() {
			return main;
			}

			public void setMain(String main) {
			this.main = main;
			}

			public String getDescription() {
			return description;
			}

			public void setDescription(String description) {
			this.description = description;
			}

			public String getIcon() {
			return icon;
			}

			public void setIcon(String icon) {
			this.icon = icon;
			}

			}
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

	
}
