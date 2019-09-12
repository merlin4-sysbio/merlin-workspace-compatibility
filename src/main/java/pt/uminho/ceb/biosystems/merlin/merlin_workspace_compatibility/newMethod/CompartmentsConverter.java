package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;

public class CompartmentsConverter {

	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void psort_reports(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			newStatement.execute("DELETE FROM compartments_annotation_reports;");
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM psort_reports;");
			
			while(rs.next()) {
				
				String query = "INSERT INTO compartments_annotation_reports VALUES (" + rs.getInt(1) + ", '" + 
				DatabaseUtilities.databaseStrConverter(rs.getString(4), newConnection.getDatabase_type()) + "', '" + rs.getString(3) + "');";
				
				newStatement.execute(query);
			}
			
			oldStatement.close();
			newStatement.close();
		} 
		catch (SQLException e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
}
