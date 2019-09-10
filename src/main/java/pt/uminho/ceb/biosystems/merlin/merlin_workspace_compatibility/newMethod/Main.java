package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.io.IOException;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.H2DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;

public class Main {

	public static void main(String[] args) {
		
		String old_host = "";
		String old_databaseName = "";
		String old_password = "";
		String old_port = "";
		String old_username = "";
		DatabaseType old_type = null;
		
		String new_host = "";
		String new_databaseName = "";
		String new_password = "";
		String new_port = "";
		String new_username = "";		
		DatabaseType new_type = null;
		
//		DatabaseAccess oldConnection = generateDBAccess(old_host, old_databaseName, old_password, old_port, old_username, old_type);
//		
//		DatabaseAccess newConnection = generateDBAccess(new_host, new_databaseName, new_password, new_port, new_username, new_type);
	
		Connection oldConnection = null;
		
		Connection newConnection = null;
		
		Merlin3ToMerlin4 converter = new Merlin3ToMerlin4(oldConnection, newConnection);
		
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
