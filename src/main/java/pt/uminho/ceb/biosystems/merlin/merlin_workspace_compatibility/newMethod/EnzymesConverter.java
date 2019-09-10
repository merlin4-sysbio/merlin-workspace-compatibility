package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;

public class EnzymesConverter {

	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void geneHomology(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement oldStatement2 = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM geneHomology;");
			
			while(rs.next()) {
				
				ResultSet rs2 = oldStatement.executeQuery("SELECT sequence FROM fastaSequence WHERE geneHomology_s_key  = " + rs.getString(1) + ";");
				
				rs2.next();
				
				String sequence = rs2.getString(1);
				
				if(sequence != null) {
					
					rs2 = oldStatement.executeQuery("SELECT idsequence FROM sequence WHERE sequence = " + sequence + ";");
					
					rs2.next();
					
					Integer sequenceId = rs.getInt(1);
					
					newStatement.execute("INSERT INTO enzymes_annotation_geneHomology (skey, homologySetup_s_key, locusTag, query, gene, chromosome, organelle, uniprot_star, status, uniprot_ecnumber, model_sequence_idsequence) VALUES ("
							+ rs.getString(1) + ", " + rs.getString(2) + ", " + rs.getString(3) + ", " + rs.getString(4) + ", " + rs.getString(5) + ", " + rs.getString(6) +
							", " + rs.getString(7) + ", " + rs.getString(8) + ", " + rs.getString(9) + ", " + rs.getString(10) + ", " + sequenceId +");");
					
				}
				
				rs2.close();
				
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
