import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DBRunner {
	
	public static int currencyCount;

	public static void main(String[] args) {
		//rebuildTables();
		
		LocalDateTime start = LocalDateTime.now();
		LocalDateTime hourEnd = start.plusHours(1).truncatedTo(ChronoUnit.HOURS);
		long hour = Duration.between(start, hourEnd).toMillis();
		HourlyDBPush p = new HourlyDBPush();
		
		LocalDateTime dayEnd = start.plusDays(2).truncatedTo(ChronoUnit.DAYS);
		long twoDay = Duration.between(start, dayEnd).toMillis();
		DailyDBPush q = new DailyDBPush();
		
		System.out.println("Crypto Database Runner Started");
		
		System.out.println("Hourly uploads will begin in " + (hour/1000) + " seconds");
		System.out.println("Daily uploads will begin in " + (((twoDay/1000)/60)/60) + " hours");
		ScheduledFuture<?> hourScheduler;
		ScheduledFuture<?> weekScheduler;
		ScheduledExecutorService hourBasedOperator = Executors.newScheduledThreadPool(1);
		if(args == null || args.length == 0) hourScheduler = hourBasedOperator.scheduleAtFixedRate(p, hour, 3600000/6, TimeUnit.MILLISECONDS);
		else if(args[0].equalsIgnoreCase("TEST")) hourScheduler = hourBasedOperator.scheduleAtFixedRate(p, 0, 10, TimeUnit.SECONDS);
		
		
		ScheduledExecutorService weekBasedOperator = Executors.newScheduledThreadPool(1);
		if(args == null || args.length == 0) weekScheduler = weekBasedOperator.scheduleAtFixedRate(q, twoDay, 604800000, TimeUnit.MILLISECONDS);
		else if(args[0].equalsIgnoreCase("TEST")) weekScheduler = weekBasedOperator.scheduleAtFixedRate(q, 1440, 1440, TimeUnit.SECONDS);
		
		
	}
	
	public static void rebuildTables() {
		try {
			Connection dbConn = DBConnector.getRemoteConnection();
			
			if(dbConn != null) {
				System.out.println("Database connection successfully created");
			} else {
				System.out.println("Database connection failed");
				return;
			}
			
			String[] dat = new HourlyDBPush().getCodes();
			
			for(int i = 0; i < dat.length; i++) {
				String query = "ALTER TABLE Hourly0 ADD "+ dat[i] + " decimal(11, 6)";
				String query1 = "ALTER TABLE Daily ADD "+ dat[i] + " decimal(11, 6)";
				PreparedStatement ps = dbConn.prepareStatement(query);
				ps.execute();
			}
			
			dbConn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
