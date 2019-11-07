package pt.uminho.ceb.biosystems.merlin.converter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;

public class Projects {
	
	private static final Logger logger = LoggerFactory.getLogger(ModelConverter.class);
	private static final int LIMIT = 3;
	
	/**
	 * @param oldTable
	 * @param newTable
	 * @throws InterruptedException 
	 */
	public static Integer projects(Connection oldConnection, Connection newConnection, int error) throws InterruptedException {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			newStatement.execute("DELETE FROM projects;");

			DatabaseType type = newConnection.getDatabase_type();

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM projects;");
			
			Integer id = null;
			Integer taxId = null;
			String compTool = null;

			while(rs.next()) {

				try {
					
					taxId = rs.getInt("organism_id");
					
					boolean latest = false;
					
					if(rs.getInt("latest_version") == 1)
						latest = true;
					
					if(latest) {
						if(id == null) {
							id = rs.getInt("id");
							taxId = rs.getInt("organism_id");
							compTool = str(rs.getString("compartments_tool"), type);
						}
						else if(compTool == null && str(rs.getString("compartments_tool"), type) != null){
							newStatement.execute("UPDATE projects SET latest_version = FALSE WHERE id = " + id + ";");
							id = rs.getInt("id");
							taxId = rs.getInt("organism_id");
							compTool = str(rs.getString("compartments_tool"), type);
						}
						else if(id != null)
							latest = false;
					}
				
					newStatement.execute("INSERT INTO projects (id, organism_id, latest_version, date, project_version,"
							+ " organism_name, organism_lineage, compartments_tool) VALUES (" + rs.getInt("id") + ", " + rs.getInt("organism_id") 
							+ ", " + latest + ", " +  str(rs.getString("date"), type) + ", " + str(rs.getString("version"), type)
							+ ", " +  str(rs.getString("organism_name"), type) + ", " +  str(rs.getString("organism_lineage"), type) + ", " +  str(rs.getString("compartments_tool"), type) + ");");
					

				} catch (JdbcSQLIntegrityConstraintViolationException e) {
//					System.out.println("Primary key constraint violation geneId = " + geneId + " and proteinId = " + proteinId);
					//					e.printStackTrace();
				}
				catch (MySQLIntegrityConstraintViolationException e) {
//					System.out.println("Primary key constraint violation geneId = " + geneId + " and proteinId = " + proteinId);
					//					e.printStackTrace();
				}
				catch (CommunicationsException e) {

					if(error < LIMIT) {

						logger.error("Communications exception! Retrying...");

						TimeUnit.MINUTES.sleep(1);

						error++;

						oldConnection = new Connection(oldConnection.getDatabaseAccess());
						newConnection = new Connection(newConnection.getDatabaseAccess());

						projects(oldConnection, newConnection, error);
					}
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
			}

			rs.close();

			oldStatement.close();
			newStatement.close();
			
			return taxId;
		} 
		catch (SQLException e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
		return null;
		
	}

	public static String str(String word, DatabaseType type) {
	
		if(word == null || word.equalsIgnoreCase("null"))
		return null;
	
		return "'" + DatabaseUtilities.databaseStrConverter(word, type) + "'";
	
	}

}
