package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.uvigo.ei.aibench.workbench.Workbench;
import jersey.repackaged.com.google.common.collect.Lists;
import pt.uminho.ceb.biosystems.merlin.aibench.operations.transporters.transyt.IntegrateTransportersDataTransyt;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseUtilities;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;
import pt.uminho.ceb.biosystems.merlin.utilities.DatabaseFilesPaths;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;

public class Merlin3ToMerlin4 {

	Connection oldConnection;
	Connection newConnection;
	List<String> enzymesTables;
	List<String> compartmentsTables;
	List<String> interproTables;
	List<String> modelTables;

	private static final Logger logger = LoggerFactory.getLogger(Merlin3ToMerlin4.class);

	public Merlin3ToMerlin4(Connection oldConnection, Connection newConnection) {

		this.oldConnection = oldConnection;
		this.newConnection = newConnection;

	}

	public void start() throws IOException {

		logger.info("reading tables names...");

		readTablesNames();

		logger.info("reading projects table...");

		//		convertProjects();

		logger.info("reading compartments tables...");

		convertCompartments();

		logger.info("reading interpro tables...");

		convertInterpro();

		logger.info("reading model tables...");

		convertModel();

		logger.info("reading enzymes tables...");

		convertEnzymes();
	}

	/**
	 * Read the tables to execute
	 * 
	 * @throws IOException
	 */
	public void readTablesNames() throws IOException {

		String path = DatabaseFilesPaths.getModelPath(true);
		this.modelTables = Lists.reverse(FileUtils.readLines(path));

		path = DatabaseFilesPaths.getEnzymesAnnotationPath();
		this.enzymesTables = Lists.reverse(FileUtils.readLines(path));

		path = DatabaseFilesPaths.getCompartmentsAnnotationPath();
		this.compartmentsTables = Lists.reverse(FileUtils.readLines(path));

		path = DatabaseFilesPaths.getInterproAnnotationPath();
		this.interproTables = Lists.reverse(FileUtils.readLines(path));
	}

	/**
	 * Convertion of projects table
	 */
	public void convertProjects() {

		List<Integer> positions = new ArrayList<Integer>();

		positions.add(1);
		positions.add(8);
		positions.add(4);
		positions.add(3);
		positions.add(2);
		positions.add(7);
		positions.add(6);
		positions.add(5);

		genericDataRetrieverAndInjectionRespectingOrder("projects", "projects", positions);
	}

	/**
	 * Convertion of compartments_annotation tables
	 */
	public void convertCompartments() {

		for(String newTable : this.compartmentsTables) {

			List<Integer> positions = new ArrayList<Integer>();

			if(newTable.equalsIgnoreCase("compartments_annotation_reports"))
				CompartmentsConverter.psort_reports(this.oldConnection, this.newConnection);

			else if(newTable.equalsIgnoreCase("compartments_annotation_reports_has_compartments")){
				positions.add(2);
				positions.add(1);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder("psort_reports_has_compartments", newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("compartments_annotation_compartments")){
				positions.add(1);
				positions.add(3);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder("compartments", newTable, positions);
			}
			else
				genericDataRetrieverAndInjection(newTable.replace("compartments_annotation_", ""), newTable);

		}
	}

	/**
	 * Convertion of model tables
	 */
	public void convertModel() {

		for(String newTable : this.modelTables) {

			List<Integer> positions = new ArrayList<Integer>();

			if(newTable.startsWith("model_")) {

				String oldTable = newTable.replace("model_", "");

				if(newTable.equalsIgnoreCase("model_gene")) {
					positions.add(1);
					positions.add(7);
					positions.add(5);
					positions.add(3);
					positions.add(2);
					positions.add(8);
					positions.add(9);
					positions.add(6);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_sequence")) {
					positions.add(1);
					positions.add(4);
					positions.add(5);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_feature")) {
					positions.add(1);
					positions.add(2);
					positions.add(3);
					positions.add(5);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_sequence_feature")) {
					positions.add(1);
					positions.add(2);
					positions.add(4);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_pathway")) {
					positions.add(1);
					positions.add(2);
					positions.add(5);
					positions.add(3);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("stoichiometry")) {
					positions.add(1);
					positions.add(5);
					positions.add(4);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_enzyme")) {
					positions.add(2);
					positions.add(3);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_reaction")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_activating_reaction")) {
					positions.add(2);
					positions.add(3);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_compartment")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_compartment")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_enzyme")) { //to ignore column 5
					positions.add(1);
					positions.add(2);
					positions.add(3);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_reaction_has_enzyme")) {
					positions.add(2);
					positions.add(3);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_gene_has_compartment")) {
					positions.add(2);
					positions.add(1);
					positions.add(3);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_module")) {
					positions.add(1);
					positions.add(6);
					positions.add(3);
					positions.add(7);
					positions.add(5);
					positions.add(2);
					positions.add(4);
					positions.add(8);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_compound")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_modules_has_compound")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_aliases")) {
					positions.add(1);
					positions.add(4);
					positions.add(2);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_dictionary")) {
					positions.add(2);
					positions.add(1);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_protein")) {
					positions.add(1);
					positions.add(3);
					positions.add(4);
					positions.add(5);
					positions.add(6);
					positions.add(7);
					positions.add(8);
					positions.add(2);
					positions.add(9);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_compound")) {
					positions.add(1);
					positions.add(9);
					positions.add(5);
					positions.add(4);
					positions.add(6);
					positions.add(11);
					positions.add(3);
					positions.add(7);
					positions.add(2);
					positions.add(8);
					positions.add(10);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_dblinks")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);
					positions.add(4);


					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
				}
				else if(newTable.equalsIgnoreCase("model_subunit")) {
					ModelConverter.subunit(this.oldConnection, this.newConnection);
				}
				else if(newTable.equalsIgnoreCase("model_reaction")) {
					ModelConverter.reaction(this.oldConnection, this.newConnection);
				}

				else if(!newTable.equalsIgnoreCase("model_reaction_labels"))
					genericDataRetrieverAndInjection(oldTable, newTable);

			}

		}
	}

	/**
	 * Convertion of model tables
	 */
	public void convertEnzymes() {

		for(String newTable : this.enzymesTables) {

			List<Integer> positions = new ArrayList<Integer>();

			String oldTable = newTable.replace("enzymes_annotation_", "");

			if(newTable.equalsIgnoreCase("enzymes_annotation_organism")) {
				positions.add(1);
				positions.add(2);
				positions.add(4);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_productRank_has_organism")) {
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_ecNumberRank")) {
				positions.add(1);
				positions.add(3);
				positions.add(4);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_ecNumberRank")) {
				positions.add(1);
				positions.add(3);
				positions.add(4);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologues_has_ecNumber")) {
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_ecNumberList")) {
				positions.add(1);
				positions.add(3);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_productList")) {
				positions.add(1);
				positions.add(3);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_scorerConfig")) {
				positions.add(4);
				positions.add(3);
				positions.add(9);
				positions.add(5);
				positions.add(7);
				positions.add(8);
				positions.add(6);
				positions.add(1);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologyData")) {
				positions.add(1);
				positions.add(8);
				positions.add(6);
				positions.add(2);
				positions.add(4);
				positions.add(3);
				positions.add(9);
				positions.add(5);
				positions.add(7);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologySetup")) {
				positions.add(1);
				positions.add(4);
				positions.add(5);
				positions.add(8);
				positions.add(6);
				positions.add(9);
				positions.add(2);
				positions.add(3);
				positions.add(7);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologues")) {
				positions.add(1);
				positions.add(5);
				positions.add(4);
				positions.add(3);
				positions.add(7);
				positions.add(6);
				positions.add(8);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_geneHomology_has_homologues")) {
				positions.add(1);
				positions.add(2);
				positions.add(6);
				positions.add(5);
				positions.add(4);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_geneHomology")) {
				EnzymesConverter.geneHomology(this.oldConnection, this.newConnection);
			}
			else
				genericDataRetrieverAndInjection(oldTable, newTable);

		}

	}

	/**
	 * Convertion of interpro tables
	 */
	public void convertInterpro() {

		for(String newTable : this.interproTables) {

			List<Integer> positions = new ArrayList<Integer>();

			if(newTable.equalsIgnoreCase("interpro_result_has_entry")){
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("interpro_result_has_model")){
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("interpro_xRef")){
				positions.add(1);
				positions.add(2);
				positions.add(3);
				positions.add(5);
				positions.add(4);
				positions.add(6);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("interpro_location")){
				positions.add(1);
				positions.add(3);
				positions.add(9);
				positions.add(8);
				positions.add(7);
				positions.add(6);
				positions.add(10);
				positions.add(5);
				positions.add(4);
				positions.add(2);
				positions.add(11);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("interpro_results")){
				positions.add(1);
				positions.add(4);
				positions.add(5);
				positions.add(6);
				positions.add(2);
				positions.add(3);
				positions.add(7);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else if(newTable.equalsIgnoreCase("interpro_result")){
				positions.add(1);
				positions.add(6);
				positions.add(11);
				positions.add(8);
				positions.add(3);
				positions.add(5);
				positions.add(9);
				positions.add(10);
				positions.add(7);
				positions.add(4);
				positions.add(2);
				positions.add(12);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions);
			}
			else
				genericDataRetrieverAndInjection(newTable, newTable);

		}
	}

	/**
	 * @param oldTable
	 * @param newTable
	 */
	public void genericDataRetrieverAndInjection(String oldTable, String newTable) {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			newStatement.execute("DELETE FROM " + newTable + ";");

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM " + oldTable + ";");

			ResultSetMetaData rsmd = rs.getMetaData();

			int columns = rsmd.getColumnCount();

			while(rs.next()) {

				int col = 1;

				String query = "INSERT INTO " + newTable + " VALUES (";

				while(col <= columns) {

					String data = null;

					if(rs.getString(col) != null)
						data = "'" + DatabaseUtilities.databaseStrConverter(rs.getString(col), newConnection.getDatabase_type()) + "'";

					query +=  data ;

					if(col < columns)
						query += ", ";

					col++;
				}

				query += ");";

				newStatement.execute(query);
			}

			oldStatement.close();
			newStatement.close();
		} 
		catch (SQLException e) {

			if(oldTable.equalsIgnoreCase("protein_complex"))
				logger.warn("This database does not contain any protein_complex table!");
			else

				//			Workbench.getInstance().error(e);
				e.printStackTrace();
		}
	}

	/**
	 * @param oldTable
	 * @param newTable
	 */
	public void genericDataRetrieverAndInjectionRespectingOrder(String oldTable, String newTable, List<Integer> positions) {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			newStatement.execute("DELETE FROM " + newTable + ";");

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM " + oldTable + ";");

			while(rs.next()) {

				String query = "INSERT INTO " + newTable + " VALUES (";

				int count = 1;

				for(int pos : positions) {

					String data = null;

					if(rs.getString(pos) != null)
						data = "'" + DatabaseUtilities.databaseStrConverter(rs.getString(pos), newConnection.getDatabase_type()) + "'";

					query +=  data ;

					if(count < positions.size())
						query += ", ";

					count++;
				}

				query += ");";

				newStatement.execute(query);
			}

			oldStatement.close();
			newStatement.close();
		} 
		catch (SQLException e) {
			
			System.out.println(newTable);
			
			//			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
}
