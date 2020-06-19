import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class DBConnector {
	
	public static Connection getRemoteConnection() {
		try {
	      Class.forName("com.mysql.cj.jdbc.Driver");
	      String dbName = "crypto";
	      String userName = System.getenv("RDS_USER");
	      String password = System.getenv("RDS_PASSWORD");
	      String hostname = System.getenv("RDS_HOSTNAME");
	      String port = "3306";
	      String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
	      System.out.println("Attempting to connect to MySQL Database...");
	      Connection con = DriverManager.getConnection(jdbcUrl);
	      return con;
	    } catch (ClassNotFoundException e) { 
	    	e.printStackTrace();
	    }
	    catch (SQLException e) { 
	    	e.printStackTrace();
	    }
	    return null;
	    
	}
	
	public static int getTime() {
		return getTime(LocalDateTime.now());
	}
	
	public static int getTime(LocalDateTime t) {
		String dt = "";
		String m = "";
		String d = "";
		String h = "";
		if(t.getMonthValue() < 10) m="0";
		if(t.getDayOfMonth() < 10) d = "0";
		if(t.getHour() < 10) h = "0";
		dt = t.getYear() + m + t.getMonthValue() + d + t.getDayOfMonth() + h + t.getHour();
		return Integer.parseInt(dt);
	}

}
