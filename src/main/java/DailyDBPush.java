import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class DailyDBPush implements Runnable {
	
	private static final int SIZE = 24;
	private Currency[] data;
 	
	public DailyDBPush() {
		data = new Currency[DBRunner.currencyCount];
	}

	@Override
	public void run() {
		
		try {
			Connection dbConn = DBConnector.getRemoteConnection();
			if(dbConn == null) return;
			String query = "SELECT * from Hourly";
			PreparedStatement pst;
			pst = dbConn.prepareStatement(query);
			ResultSet rs = pst.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			while(rs.getInt("time") < shaveToYesterday()) {
				rs.next();
			}
			
			for(int i = 0; i < data.length; i++) {
				data[i] = new Currency(rsmd.getColumnLabel(i+2), SIZE);
			}
			/*
			for(int i = 0 ; i < SIZE; i++) {
				data[i].setVal(rs.getDouble(data[i].getCode()), i);
			}
			*/
			double[][] dat = new double[SIZE][data[0].getSize()];
			for(int i = 0; i < dat.length; i++) {
				for(int j = 1; j < dat[i].length; j++) {
					dat[i][j-1] = rs.getDouble(j+1);
				}
				rs.next();
			}
			
			for(int i = 0; i < dat[0].length; i++) {
				for(int j = 0; j < dat.length; j++) {
					data[i].setVal(dat[j][i], i);
				}
			}
			
			pst.execute();
			
			String query1 = "INSERT INTO Hourly (time";
			for(int i = 0; i < data.length; i++) {
				query+= ", ";
				query += data[i].getCode();
			}
			
			query += ") VALUES (?";
			
			for(int i = 0; i < data.length; i++) {
				query += ", ?";
			}
			
			query += ")";
			
			PreparedStatement ps = dbConn.prepareStatement(query1);
			System.out.print("Pushing data: " + DBConnector.getTime());
			ps.setInt(1, DBConnector.getTime());
			for(int i = 0; i < data.length; i++) {
				if(!data[i].getCode().equals("USD")) ps.setDouble(i+2, data[i].getAverage());
				else ps.setDouble(i+2, 1.0);
				System.out.print(" | " + data[i].getCode() + ": ");
				if(!data[i].getCode().equals("USD")) System.out.print(data[i].getAverage());
				else System.out.print("1.00");
			}
			
			System.out.println(" to database");
			
			ps.execute();
			
			dbConn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
		
	}
	
	private static int shaveToYesterday() {
		LocalDateTime now = LocalDateTime.now();
		LocalDateTime yesterday = now.minusDays(1);
		return DBConnector.getTime(yesterday);
		
	}
	
	

}
