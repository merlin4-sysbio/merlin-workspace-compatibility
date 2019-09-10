package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import pt.uminho.ceb.biosystems.merlin.aibench.utilities.LoadFromConf;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.utilities.datastructures.map.MapUtils;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;

public class WorkspaceConverter {

	public static void start(Connection connection){

		try {
			renameAllTables(connection);

			updateModelGeneColumn(connection);
			
			dropChromosomeTable(connection);
			
			deleteNumberOfChainsFromStoichiometry(connection);

			createModelReactionLabelsTable(connection);
			
			createForeignKeyInModelReactionTable(connection); //before setting the foreign key, data must be inserted
			
			splitModelReactionTable(connection);

			setForeignKeyInModelReactionTable(connection);

		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




	}
	
	/**
	 * @param connection
	 */
	public static void splitModelReactionTable(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("INSERT INTO model_reaction_labels (name, equation, isGeneric, isSpontaneous, isNonEnzymatic, source) "
					+ "SELECT name, equation, isGeneric, isSpontaneous, isNonEnzymatic, source FROM model_reaction;");
			
			ResultSet rs = stmt.executeQuery("SELECT idreaction_label, name, equation FROM model_reaction_labels;");
			
			while(rs.next())
				stmt.execute("UPDATE model_reaction SET reaction_labels_idreaction_label = " + rs.getInt(1) + " WHERE name = " + rs.getString(2) + " AND equation = " + rs.getString(3) + ";");
			
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `name`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `equation`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `isGeneric`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `isSpontaneous`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `isNonEnzymatic`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `source`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `reversible`;");
			stmt.execute("ALTER TABLE `model_reaction` DROP COLUMN `originalReaction`;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * @param connection
	 */
	public static void deleteNumberOfChainsFromStoichiometry(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("ALTER TABLE `model_stoichiometry` DROP COLUMN `numberofchains`;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * @param connection
	 */
	public static void createForeignKeyInModelReactionTable(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("ALTER TABLE `model_reaction` ADD COLUMN `reaction_labels_idreaction_label` INT UNSIGNED NOT NULL AFTER `idreaction`;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param connection
	 */
	public static void setForeignKeyInModelReactionTable(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("ALTER TABLE `model_reaction` ADD " +
					"  CONSTRAINT `fk_model_reaction_model_reaction_labels1`" + 
					"    FOREIGN KEY (`reaction_labels_idreaction_label`)" + 
					"    REFERENCES `model_reaction_labels` (`idreaction_label`)" + 
					"    ON DELETE CASCADE" + 
					"    ON UPDATE CASCADE;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * @param connection
	 */
	public static void createModelReactionLabelsTable(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("CREATE TABLE IF NOT EXISTS `model_reaction_labels` (" + 
					"  `idreaction_label` INT UNSIGNED NOT NULL AUTO_INCREMENT," + 
					"  `name` VARCHAR(400) NULL," + 
					"  `equation` TEXT NULL," + 
					"  `isGeneric` TINYINT(1) NULL," + 
					"  `isSpontaneous` TINYINT(1) NULL," + 
					"  `isNonEnzymatic` TINYINT(1) NULL," + 
					"  `source` VARCHAR(45) NOT NULL," + 
					"  PRIMARY KEY (`idreaction_label`))" + 
					"ENGINE = InnoDB" + 
					"PACK_KEYS = 0" + 
					"ROW_FORMAT = DEFAULT;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * @param connection
	 */
	public static void dropChromosomeTable(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("DROP TABLE IF EXISTS `chromosome`;");
			
			stmt.execute("DROP TABLE IF EXISTS `model_chromosome`;");
			
			stmt.execute("ALTER TABLE `model_gene` DROP COLUMN `chromosome_idchromosome`;");
			
			stmt.execute("ALTER TABLE `model_gene` " + 
				"CHANGE COLUMN `sequence_id` `query` VARCHAR(45) NOT NULL ;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * @param connection
	 */
	public static void updateModelGeneColumn(Connection connection){

		try {
			Statement stmt = connection.createStatement();
			
			stmt.execute("ALTER TABLE `model_gene` DROP COLUMN `chromosome_idchromosome`;");

			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param connection
	 */
	public static void renameAllTables(Connection connection){

		try {

			String path = FileUtils.getUtilitiesFolderPath() + "alter_table_names.txt";

			Map<String, String> data = MapUtils.getInfoInFile(path, 0, 1, ",");

			Statement stmt = connection.createStatement();

			for(String oldName : data.keySet()) {

				String newName = data.get(oldName);

				try {
					DatabaseMetaData meta;
					meta = connection.getMetaData();
					ResultSet rs = meta.getTables(null, null, newName, new String[] {"TABLE","VIEW"});
					if(!rs.next()) {

						rs = meta.getTables(null, null, oldName, new String[] {"TABLE","VIEW"});
						if(rs.next()) {

							stmt.execute("RENAME TABLE "+oldName+" TO "+newName+";");

						}
					}
					rs.close();

				}
				catch (SQLException e) {
					e.printStackTrace();
				}
			}

			stmt.close();

		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

	//	chromosome,model_chromosome


}
