package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;

public class EnzymesConverter {

	private static final Logger logger = LoggerFactory.getLogger(EnzymesConverter.class);
	private static final int LIMIT = 3;
	
	/**
	 * @param oldTable
	 * @param newTable
	 * @throws InterruptedException 
	 */
	public static void geneHomology(Connection oldConnection, Connection newConnection, int error) throws InterruptedException {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement oldStatement2 = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			DatabaseType type = newConnection.getDatabase_type();

			Map<String, Integer> sequencesMapping = new HashMap<>();

			ResultSet rs = oldStatement.executeQuery("SELECT geneHomology.query, idsequence FROM sequence "
					+ "INNER JOIN gene ON idgene = gene_idgene INNER JOIN geneHomology ON sequence_id = geneHomology.query;");

			while(rs.next())
				sequencesMapping.put(str(rs.getString(1), type), rs.getInt(2));

			rs = oldStatement.executeQuery("SELECT * FROM geneHomology;");

			String newTable = "enzymes_annotation_geneHomology";

			if(type.equals(DatabaseType.H2))
				newTable = "enzymes_annotation_genehomology";

			newStatement.execute("DELETE FROM " + newTable + ";");

			String homologySetupColumnName = "homologySetup_s_key";
			String locusTagColumnName = "locusTag";

			if(type.equals(DatabaseType.H2)) {
				homologySetupColumnName = "homologysetup_s_key";
				locusTagColumnName = "locustag";
			}
			
			while(rs.next()) {

				try {

					String query = str(rs.getString(4), type);
					Integer sequenceId = null;

					if(sequencesMapping.containsKey(query))
						sequenceId = sequencesMapping.get(query);
					else {

						ResultSet rs2 = oldStatement2.executeQuery("SELECT sequence FROM fastaSequence WHERE geneHomology_s_key  = " + rs.getString(1) + ";");

						rs2.next();

						String sequence = str(rs2.getString(1), type);

						if(sequence != null) {
							
							rs2 = oldStatement2.executeQuery("SELECT idsequence FROM sequence WHERE sequence = " + sequence + ";");
							
							rs2.next();
							sequenceId = rs.getInt(1);
						}

						rs2.close();
					}

					newStatement.execute("INSERT INTO " + newTable + " (s_key, " + homologySetupColumnName + ", " + locusTagColumnName + ", query, gene, chromosome, organelle, uniprot_star, status, uniprot_ecnumber, model_sequence_idsequence) VALUES ("
							+ rs.getInt(1) + ", " + rs.getInt(2) + ", " + str(rs.getString(3), type) + ", " + query + ", " + str(rs.getString(5), type) + ", " + str(rs.getString(6), type) +
							", " + str(rs.getString(7), type) + ", " + rs.getInt(8) + ", " + str(rs.getString(9), type) + ", " + str(rs.getString(10), type) + ", " + sequenceId +");");

				} catch (JdbcSQLIntegrityConstraintViolationException e) {
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
				catch (CommunicationsException e) {

					if(error < LIMIT) {
						
						logger.error("Communications exception! Retrying...");

						TimeUnit.MINUTES.sleep(1);

						error++;
						
						oldConnection = new Connection(oldConnection.getDatabaseAccess());
						newConnection = new Connection(newConnection.getDatabaseAccess());
						
						geneHomology(oldConnection, newConnection, error);
					}
					//					System.out.println("Primary key constraint violation in table " + newTable);
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

	private static String str(String word, DatabaseType type) {

		if(word == null || word.equalsIgnoreCase("null"))
			return null;

		return "'" + DatabaseUtilities.databaseStrConverter(word, type) + "'";

	}
}
