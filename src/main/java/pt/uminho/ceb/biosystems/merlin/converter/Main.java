package pt.uminho.ceb.biosystems.merlin.converter;

import java.io.IOException;

import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.H2DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;

public class Main {

	public static void main(String[] args) {
		
		String old_host = "palsson.di.uminho.pt";
		String old_databaseName = "calbicans";
		String old_password = "dev$2018merlin";
		String old_port = "2401";
		String old_username = "merlindev";
		DatabaseType old_type = DatabaseType.MYSQL;
		
//		String new_host = "C:\\Users\\BioSystems\\merlin4\\merlin-aibench";
//		String new_databaseName = "converter_test";
//		String new_password = "password";
//		String new_port = "";
//		String new_username = "root";		
//		DatabaseType new_type = DatabaseType.H2;
		
		String new_host = "palsson.di.uminho.pt";
		String new_databaseName = "calbicans_v4";
		String new_password = "dev$2018merlin";
		String new_port = "2401";
		String new_username = "merlindev";		
		DatabaseType new_type = DatabaseType.MYSQL;
		
		DatabaseAccess oldAccess = generateDBAccess(old_host, old_databaseName, old_password, old_port, old_username, old_type);
		
		DatabaseAccess newAccess = generateDBAccess(new_host, new_databaseName, new_password, new_port, new_username, new_type);
	
		Connection oldConnection = new Connection(oldAccess);
		
		Connection newConnection = new Connection(newAccess);
		
		Merlin3ToMerlin4 converter = new Merlin3ToMerlin4(oldConnection, newConnection);
		
		oldConnection.closeConnection();
		newConnection.closeConnection();
		
		try {
			converter.start();
		} 
		catch (IOException e) {
//			Workbench.getInstance().error("An error occurred while converting the workspace!");
			e.printStackTrace();
		}

	}
	
	public static DatabaseAccess generateDBAccess(String host, String databaseName, String password, String port,
			String username, DatabaseType type) {
		
		if(type.equals(DatabaseType.MYSQL))
			return new MySQLDatabaseAccess(username, password, host, port, databaseName);
		else
			return new H2DatabaseAccess(username, password, databaseName, host);
	}

}
