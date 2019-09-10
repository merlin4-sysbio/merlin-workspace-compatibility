package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility;

import java.util.Map;

import pt.uminho.ceb.biosystems.merlin.aibench.utilities.LoadFromConf;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	
    	Connection conn = new Connection(generateDBAccess());
    	
    	WorkspaceConverter.start(conn);
    	
        System.out.println( "done" );
    }
    
	public static DatabaseAccess generateDBAccess() {
		
		Map<String, String> credentials = LoadFromConf.loadDatabaseCredentials(FileUtils.getConfFolderPath());
		
		String username = null, password = null, host = null, port = null, database = null;

		username = credentials.get("username");
		password = credentials.get("password");
		host = credentials.get("host");
		port = credentials.get("port");
		database = "hpluvialis";
		
		return new MySQLDatabaseAccess(username, password, host, port, database);
	}
}
