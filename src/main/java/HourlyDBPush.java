import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HourlyDBPush implements Runnable {
	
	public static final String COINCAP_URL = "api.coincap.io/v2/assets";
	private static final int SIZE = 6;
	public static OkHttpClient client = new OkHttpClient();
	private Currency[] currency;
	private int portion;
	
	
	public HourlyDBPush() {
		String resourceName = "/CurrencyFixture.json";
        InputStream is = DailyDBPush.class.getResourceAsStream(resourceName);
        if (is == null) {
            throw new NullPointerException("Cannot find resource file " + resourceName);
        }

        JSONTokener tokener = new JSONTokener(is);
        JSONArray array = new JSONArray(tokener);
        
        currency = new Currency[array.length()];
        
        for(int i = 0; i < currency.length; i++) {
        	String cd = array.getJSONObject(i).getString("code");
        	currency[i] = new Currency(cd, SIZE);
        }
        
        DBRunner.currencyCount = array.length();
        this.portion = 0;
	}
	
	public String[] getCodes() {
		String[] c = new String[currency.length];
		for(int i = 0 ; i < c.length; i++) {
			c[i] = currency[i].getCode();
		}
		return c;
	}

	@Override
	public void run() {
		
		JSONArray exchangeRates = getMassExchangeRate();
		
		if(portion==currency[0].getSize()) {
			writeToDB(currency);
			System.out.println(LocalDateTime.now().toString());
			portion = 0;
			clearArr(currency);
		}
		
		for(int i = 0; i < currency.length; i++) {
			for(int j = 0; j < exchangeRates.length(); j++) {
				String temp = exchangeRates.getJSONObject(j).getString("symbol");
				if(currency[i].getCode().equals(temp)) {
					currency[i].setVal(exchangeRates.getJSONObject(j).getDouble("priceUsd"), portion); 
					
				}
				
			}
		} 
		//writeToDB(interval);
		portion++;
		
		
	}
	
	private static void clearArr(Currency[] arr) {
		for(int i = 0 ; i < arr.length; i++) {
			for(int j = 1; j < arr[i].getSize(); j++) {
				arr[i].clearArr();
			}
		}
	}
	
	public static void writeToDB(Currency[] pass) {
		
		try {
			Connection dbConn = DBConnector.getRemoteConnection();
			
			if(dbConn != null) {
				System.out.println("Database connection successfully created");
			} else {
				System.out.println("Database connection failed");
				return;
			}
			
			String query = "INSERT INTO Hourly (time";
			for(int i = 0; i < pass.length; i++) {
				query+= ", ";
				query += pass[i].getCode();
			}
			
			query += ") VALUES (?";
			
			for(int i = 0; i < pass.length; i++) {
				query += ", ?";
			}
			
			query += ")";
			
			PreparedStatement ps = dbConn.prepareStatement(query);
			System.out.print("Pushing data: " + DBConnector.getTime());
			ps.setInt(1, DBConnector.getTime());
			for(int i = 0; i < pass.length; i++) {
				if(!pass[i].getCode().equals("USD")) ps.setDouble(i+2, pass[i].getAverage());
				else ps.setDouble(i+2, 1.0);
				System.out.print(" | " + pass[i].getCode() + ": ");
				if(!pass[i].getCode().equals("USD")) System.out.print(pass[i].getAverage());
				else System.out.print("1.00");
			}
			
			System.out.println(" to database");
			
			ps.execute();
			
			dbConn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static JSONArray getMassExchangeRate() {
		Request request = buildCoinCapRequest();
		
		Response res=null;
		JSONArray arr = null;
		try {
			res = sendRequest(request);
			JSONObject par = new JSONObject(res.body().string());
			arr = par.getJSONArray("data");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return arr;
	}
	
	public static Response sendRequest(Request request) throws IOException {
		Response response = client.newCall(request).execute();
		if(!response.isSuccessful()) {
			throw new IOException("API Call not successful. Error code: " + response.code() + ": " + response.message() + " " + response.body().string());
		} else {
			System.out.println(response.code() + ": " + response.message());
			return response;
		}
	}
	
	public static Request buildCoinCapRequest() {
		String url = "http://" + COINCAP_URL + "?limit=250";
		System.out.println("Attempting to GET data from " + url);
		Request request = new Request.Builder()
				.addHeader("Accept-Encoding", "deflate")
				.url(url)
				.build();
		return request;
	}

}
