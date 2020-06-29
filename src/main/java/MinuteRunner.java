import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

import org.json.JSONArray;
import org.json.JSONTokener;

public class MinuteRunner implements Runnable {
	
	private Currency[] currency;
	
	public MinuteRunner() {
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
        	currency[i] = new Currency(cd, 1);
        }
        
	}

	@Override
	public void run() {
		JSONArray exchangeRates = HourlyDBPush.getMassExchangeRate();
		
		writeToDB(currency);
		System.out.println(LocalDateTime.now().toString());
		
		for(int i = 0; i < currency.length; i++) {
			for(int j = 0; j < exchangeRates.length(); j++) {
				String temp = exchangeRates.getJSONObject(j).getString("symbol");
				if(currency[i].getCode().equals(temp)) {
					currency[i].setVal(exchangeRates.getJSONObject(j).getDouble("priceUsd"), 1); 
					
				}
				
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
			
			PreparedStatement ps1 = dbConn.prepareStatement("SELECT COUNT(*) FROM Minute");
			ResultSet rs = ps1.executeQuery();
			rs.next();
			if(rs.getInt("rowcount") > 120) {
				
			}
			
			dbConn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}

}
