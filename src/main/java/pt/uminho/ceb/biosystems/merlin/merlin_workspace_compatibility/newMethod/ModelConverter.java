package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;

public class ModelConverter {
	
	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void subunit(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement oldStatement2 = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			newStatement.execute("DELETE FROM model_subunit;");
			
			DatabaseType type = newConnection.getDatabase_type();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM subunit;");
			
			ResultSetMetaData rsmd = rs.getMetaData();

			int columns = rsmd.getColumnCount();
			
			while(rs.next()) {
				
				Integer geneId = rs.getInt(1);
				
				String gprStatus = null;
				
				String ec =  str(rs.getString(3), type);
				Integer proteinId = rs.getInt(2);
				
				try {
					
					ResultSet rs2 = oldStatement2.executeQuery("SELECT gpr_status FROM enzyme WHERE protein_idprotein = " + proteinId + " AND ecnumber = " + ec + ";");
					
					if(rs2.next())
						gprStatus = str(rs2.getString(1), type);
					
					String firstHalf = "INSERT INTO model_subunit (model_gene_idgene, model_protein_idprotein";
					
					String otherHalf = " VALUES (" + geneId + ", " + proteinId;
							
					if(columns > 3)	{
						firstHalf += ", model_module_id, gpr_status, note";
						otherHalf += ", " + str(rs.getString(4), type) + ", " + gprStatus + ", " + str(rs.getString(6), type); 
							
					}
							
							
					String query = firstHalf + ")" + otherHalf + ");";
					
					newStatement.execute(query);
					
					rs2.close();
				} catch (JdbcSQLIntegrityConstraintViolationException e) {
					System.out.println("Primary key constraint violation geneId = " + geneId + " and proteinId = " + proteinId);
//					e.printStackTrace();
				}
				catch (MySQLIntegrityConstraintViolationException e) {
					System.out.println("Primary key constraint violation geneId = " + geneId + " and proteinId = " + proteinId);
//					e.printStackTrace();
				}
			}
			
			rs.close();
			
			oldStatement.close();
			oldStatement2.close();
			newStatement.close();
		} 
		catch (SQLException e) {
//			Workbench.getInstance().error(e);
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
			
			DatabaseType type = newConnection.getDatabase_type();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM reaction;");
			
			newStatement.execute("DELETE FROM model_reaction_labels;");
			newStatement.execute("DELETE FROM model_reaction;");
			
			while(rs.next()) {
				
				ResultSet rs2 = newStatement.executeQuery("SELECT idreaction_label FROM model_reaction_labels WHERE name = " + str(rs.getString(2), type) + ";");
				
				Integer labelId = null;
				
				if(rs2.next())
					labelId = rs2.getInt(1);
				
				String isGenericColumnName = "isGeneric";
				String isNonEnzymaticColumnName = "isNonEnzymatic";
				String isSpontaneousColumnName = "isSpontaneous";
				
				if(newConnection.getDatabase_type().equals(DatabaseType.H2)) {
					isGenericColumnName = "isgeneric";
					isNonEnzymaticColumnName = "isnonenzymatic";
					isSpontaneousColumnName = "isspontaneous";
				}
				
				if(labelId == null) {
					newStatement.execute("INSERT INTO model_reaction_labels (equation, " + isGenericColumnName + ", " + isNonEnzymaticColumnName + ", " + isSpontaneousColumnName + ", name, source) VALUES ("
							+ str(rs.getString(3), type) + ", " + rs.getInt(7) + ", " + rs.getInt(9) + ", " + rs.getInt(8) + ", " + str(rs.getString(2), type) + ", " + str(rs.getString(10), type) +");");
					
					ResultSet rs3 = newStatement.executeQuery("SELECT LAST_INSERT_ID()");
					rs3.next();
					
					labelId = rs3.getInt(1);
					
					rs3.close();
				}
				
				rs2.close();
				
				String inModelColumnName = "inModel";
				String lowerBoundColumnName = "lowerBound";
				String upperBoundColumnName = "upperBound";
				
				if(newConnection.getDatabase_type().equals(DatabaseType.H2)) {
					inModelColumnName = "inmodel";
					lowerBoundColumnName = "lowerbound";
					upperBoundColumnName = "upperbound";
				}
				
				newStatement.execute("INSERT INTO model_reaction (idreaction, boolean_rule, " + inModelColumnName + ", " + lowerBoundColumnName + ", notes, " + upperBoundColumnName + 
						", compartment_idcompartment, model_reaction_labels_idreaction_label) VALUES ("
						+ rs.getInt(1) + ", " + str(rs.getString(5), type) + ", " + rs.getInt(6) + ", " + str(rs.getString(14), type) + ", " + str(rs.getString(13), type) + ", "
						+ str(rs.getString(15), type) + ", " + str(rs.getString(12), type) + ", " + labelId + ");");
				
			}
			
			rs.close();
			
			oldStatement.close();
			oldStatement2.close();
			newStatement.close();
		} 
		catch (SQLException e) {
//			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
	
	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void protein(Connection oldConnection, Connection newConnection) {
		
		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement oldStatement2 = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			newStatement.execute("DELETE FROM model_protein;");
			
			DatabaseType type = newConnection.getDatabase_type();
			
			ResultSet rs = oldStatement.executeQuery("SELECT * FROM protein;");
			
			while(rs.next()) {
				
				Integer proteinId = rs.getInt(1);
				
				ResultSet rs2 = oldStatement2.executeQuery("SELECT * FROM enzyme WHERE protein_idprotein = " + proteinId + " ;");
				
				String ec = null;
				String source = null;
				Integer inModel = null;
				
				if(rs2.next()) {
					ec = str(rs2.getString(1), type);
					source = str(rs2.getString(4), type);
					inModel = rs2.getInt(3);
				}
				
				String inModelColumnName = "inModel";
				
				if(newConnection.getDatabase_type().equals(DatabaseType.H2))
					inModelColumnName = "inmodel";
				
				String query = "INSERT INTO model_protein (idprotein, class, ecnumber, " + inModelColumnName + ", inchi, molecular_weight, molecular_weight_exp, "
						+ "molecular_weight_kd, molecular_weight_seq, name, pi, source)  VALUES (" + proteinId + ", " + str(rs.getString(3), type) 
						+ ", " + ec + ", " + inModel + ", " + str(rs.getString(4), type) + ", " + str(rs.getString(5), type) + ", "
						+ str(rs.getString(6), type) + ", " + str(rs.getString(7), type) + ", " + str(rs.getString(8), type) + ", " + str(rs.getString(2), type) 
						+ ", " + str(rs.getString(9), type) + ", " + source + ");";
				
				newStatement.execute(query);
				
				rs2.close();
			}
			
			rs.close();
			
			oldStatement.close();
			oldStatement2.close();
			newStatement.close();
		} 
		catch (SQLException e) {
//			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
	
	public static String str(String word, DatabaseType type) {
		
		if(word == null || word.equalsIgnoreCase("null"))
			return null;
		
		return "'" + DatabaseUtilities.databaseStrConverter(word, type) + "'";
		
	}
}
