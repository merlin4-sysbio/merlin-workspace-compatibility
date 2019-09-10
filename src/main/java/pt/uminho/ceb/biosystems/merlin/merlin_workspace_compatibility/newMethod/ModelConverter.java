package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;

public class ModelConverter {
	
	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void subunit(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM subunit;");
			
			ResultSetMetaData rsmd = rs.getMetaData();

			int columns = rsmd.getColumnCount();
			
			while(rs.next()) {
				
				String query = "INSERT INTO model_subunit (" + rs.getString(3) + ", " + rs.getString(2) + ", " + rs.getString(1);
						
				if(columns > 3)		
						query += ", " + rs.getString(4) + ", " + rs.getString(5) + ", " + rs.getString(6) + ", " + rs.getString(7); 
						
						
				query += ");";
				
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
	
	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void reaction(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement oldStatement2 = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM reaction;");
			
			while(rs.next()) {
				
				ResultSet rs2 = oldStatement.executeQuery("SELECT idreaction_label FROM model_reaction_labels WHERE name = " + rs.getString(2) + ";");
				
				Integer labelId = null;
				
				if(rs2.next())
					labelId = rs2.getInt(1);
				
				if(labelId == null) {
					newStatement.execute("INSERT INTO model_reaction_labels (equation, isGeneric, isNonEnzymatic, isSpontaneous, name, source) VALUES ("
							+ rs.getString(3) + ", " + rs.getString(7) + ", " + rs.getString(9) + ", " + rs.getString(8) + ", " + rs.getString(2) + ", " + rs.getString(10) +");");
					
					ResultSet rs3 = newStatement.executeQuery("SELECT LAST_INSERT_ID()");
					rs3.next();
					
					labelId = rs3.getInt(1);
					
					rs3.close();
				}
				
				rs2.close();
				
				newStatement.execute("INSERT INTO model_reaction (idreaction, boolean_rule, inModel, lowerBound, notes, upperBound, compartment_idcompartment, model_reaction_labels_idreaction_labels) VALUES ("
						+ rs.getString(1) + ", " + rs.getString(5) + ", " + rs.getString(6) + ", " + rs.getString(14) + ", " + rs.getString(13) + ", " + rs.getString(15) + ", " + rs.getString(12) + "," + labelId + ");");
				
			}
			
			rs.close();
			
			oldStatement.close();
			oldStatement2.close();
			newStatement.close();
		} 
		catch (SQLException e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
}
