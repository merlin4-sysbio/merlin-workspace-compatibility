package pt.uminho.ceb.biosystems.merlin.converter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;

public class CompartmentsConverter {

//	/**
//	 * @param oldTable
//	 * @param newTable
//	 */
//	public static void psort_reports(Connection oldConnection, Connection newConnection) {
//
//		try {
//			Statement oldStatement = oldConnection.createStatement();
//			Statement newStatement = newConnection.createStatement();
//
//			newStatement.execute("DELETE FROM compartments_annotation_reports;");
//
//			ResultSet rs = oldStatement.executeQuery("SELECT * FROM psort_reports;");
//
//			while(rs.next()) {
//
//				String date = null;
//				String locus = null;
//
//				if(rs.getString(4) != null)
//					date =  str(rs.getString(4), newConnection.getDatabase_type());
//
//				if(rs.getString(3) != null)
//					locus = str(rs.getString(3), newConnection.getDatabase_type());
//
//				String query = "INSERT INTO compartments_annotation_reports VALUES (" + rs.getInt(1) + ", " + 
//						date + ", " + locus + ");";
//
//				newStatement.execute(query);
//			}
//
//			oldStatement.close();
//			newStatement.close();
//		} 
//		catch (JdbcSQLIntegrityConstraintViolationException e) {
//			//					System.out.println("Primary key constraint violation in table " + newTable);
//			//											e.printStackTrace();
//		}
//		catch (MySQLIntegrityConstraintViolationException e) {
//			//	Workbench.getInstance().error(e);
//			e.printStackTrace();
//		}
//		catch (SQLException e) {
//			Workbench.getInstance().error(e);
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * @param oldTable
	 * @param newTable
	 */
	public static void psort_reports(Connection oldConnection, Connection newConnection) {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();
			
			Map<String, Integer> ids = new HashMap<>();
			
			ResultSet rs = oldStatement.executeQuery("SELECT sequence_id, idgene FROM gene;");
			
			while(rs.next())
				ids.put(rs.getString(1), rs.getInt(2));
			
			newStatement.execute("DELETE FROM compartments_annotation_reports;");

			rs = oldStatement.executeQuery("SELECT locus_tag, compartment_id, score FROM psort_reports INNER JOIN psort_reports_has_compartments "
					+ "ON psort_reports.id = psort_reports_has_compartments.psort_report_id;");

			while(rs.next()) {
				
				String locusTag = rs.getString(1);
				Integer compartmentId = rs.getInt(2);
				
				Integer geneId = ids.get(locusTag);
				
				String score = str(rs.getString(2), newConnection.getDatabase_type());
				
				if(geneId != null) {
					
					newStatement.execute("INSERT INTO compartments_annotation_reports_has_compartments (model_gene_idgene, compartments_annotation_compartments_id, score) "
							+ "VALUES (" + geneId + ", " + compartmentId + ", " + score + ";)");
					
				}
			}

			oldStatement.close();
			newStatement.close();
		} 
		catch (JdbcSQLIntegrityConstraintViolationException e) {
			//					System.out.println("Primary key constraint violation in table " + newTable);
			//											e.printStackTrace();
		}
		catch (MySQLIntegrityConstraintViolationException e) {
			//	Workbench.getInstance().error(e);
			e.printStackTrace();
		}
		catch (SQLException e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}

	/**
	 * @param word
	 * @param type
	 * @return
	 */
	public static String str(String word, DatabaseType type) {

		if(word == null || word.equalsIgnoreCase("null"))
			return null;

		return "'" + DatabaseUtilities.databaseStrConverter(word, type) + "'";

	}
}
