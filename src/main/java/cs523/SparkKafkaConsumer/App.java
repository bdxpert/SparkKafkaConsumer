package cs523.SparkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;

public class App {
	 
   static String lastKey="";
   static List<String> list=new ArrayList<String>();
   public static void main(String[] args) throws Exception 
   {    
	   System.out.println("Main Called");
	   
	   CustomSpark spark=new CustomSpark();
	   MyResources resources = new MyResources();
	   WeatherKafkaConsumer consumer = new WeatherKafkaConsumer(resources);
	   consumer.Wait(()-> {
				try {
					Thread.sleep(3000);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
	   , (String key, String val,boolean new_key)-> 
	   {
		   if(lastKey.isEmpty())
			   lastKey=key;
		   if(new_key)
		   {
			   try 
			   {
				   if(!list.isEmpty())
					   spark.Process(lastKey, list);
			   } 
			   catch (Exception e) { e.printStackTrace();}
			   
			   lastKey=key;
			   list.clear();
		   }
		  // System.out.println("Added to list");
		   list.add(val);
	   });
   }
}