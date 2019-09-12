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
				
				String date = null;
				String locus = null;
				
				if(rs.getString(4) != null)
					date = "'" + DatabaseUtilities.databaseStrConverter(rs.getString(4), newConnection.getDatabase_type()) + "'";
				
				if(rs.getString(3) != null)
					locus = "'" + DatabaseUtilities.databaseStrConverter(rs.getString(2), newConnection.getDatabase_type()) + "'";
				
				String query = "INSERT INTO compartments_annotation_reports VALUES (" + rs.getInt(1) + ", " + 
				date + ", " + locus + ");";
				
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
