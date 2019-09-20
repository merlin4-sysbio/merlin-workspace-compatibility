package pt.uminho.ceb.biosystems.merlin.converter;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import es.uvigo.ei.aibench.workbench.Workbench;
import jersey.repackaged.com.google.common.collect.Lists;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;
import pt.uminho.ceb.biosystems.merlin.utilities.DatabaseFilesPaths;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;

public class Merlin3ToMerlin4 {

	private Connection oldConnection;
	private Connection newConnection;
	private List<String> enzymesTables;
	private List<String> compartmentsTables;
	private List<String> interproTables;
	private List<String> modelTables;
	private Integer counter = 0;
	
	
	private PropertyChangeSupport changes;

	private static final Logger logger = LoggerFactory.getLogger(Merlin3ToMerlin4.class);
	private static final int LIMIT = 3;

	public Merlin3ToMerlin4(Connection oldConnection, Connection newConnection) {

		this.oldConnection = oldConnection;
		this.newConnection = newConnection;
		
		this.changes = new PropertyChangeSupport(this);

	}

	public void start() throws IOException {

		try {
			logger.info("reading tables names...");

			readTablesNames();
			
			int total = this.enzymesTables.size() + this.compartmentsTables.size() + this.modelTables.size() + this.interproTables.size() + 1;
			
			this.changes.firePropertyChange("size", null, total);
			this.changes.firePropertyChange("message", null, "importing and converting data, this process may take a while");

			logger.info("importing projects table...");

			convertProjects();

			logger.info("importing compartments tables...");

			convertCompartments();

			logger.info("importing interpro tables...");

			convertInterpro();

			logger.info("importing model tables...");

			convertModel();

			logger.info("importing enzymes tables...");

			convertEnzymes();
		} 
		catch (InterruptedException e) {
			Workbench.getInstance().error(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	 * @throws InterruptedException 
	 */
	public void convertProjects() throws InterruptedException {

		List<Integer> positions = new ArrayList<Integer>();
		List<Integer> bits = new ArrayList<Integer>();

		positions.add(1);
		positions.add(8);
		positions.add(4);
		positions.add(3);
		positions.add(2);
		positions.add(7);
		positions.add(6);
		positions.add(5);

		bits.add(3);
		genericDataRetrieverAndInjectionRespectingOrderAndBitsType("projects", "projects", positions, bits, 0);
		
		counter++;
		this.changes.firePropertyChange("tablesCounter", null, counter);
	}

	/**
	 * Convertion of compartments_annotation tables
	 * @throws InterruptedException 
	 */
	public void convertCompartments() throws InterruptedException {

		for(String newTable : this.compartmentsTables) {
			
			int error = 0;

			logger.info("Table: " + newTable);

			List<Integer> positions = new ArrayList<Integer>();

			if(newTable.equalsIgnoreCase("compartments_annotation_reports"))
				CompartmentsConverter.psort_reports(this.oldConnection, this.newConnection);

			else if(newTable.equalsIgnoreCase("compartments_annotation_reports_has_compartments")){
				positions.add(2);
				positions.add(1);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder("psort_reports_has_compartments", newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("compartments_annotation_compartments")){
				positions.add(1);
				positions.add(3);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder("compartments", newTable, positions, error);
			}
			else
				genericDataRetrieverAndInjection(newTable.replace("compartments_annotation_", ""), newTable, error);
			
			counter++;
			this.changes.firePropertyChange("tablesCounter", null, counter);

		}
	}

	/**
	 * Convertion of model tables
	 * @throws InterruptedException 
	 */
	public void convertModel() throws InterruptedException {

		for(String newTable : this.modelTables) {

			List<Integer> positions = new ArrayList<Integer>();
			List<Integer> bits = new ArrayList<Integer>();

			logger.info("Table: " + newTable);
			
			int error = 0;

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

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_sequence")) {
					positions.add(1);
					positions.add(4);
					positions.add(5);
					positions.add(3);
					positions.add(2);

					ModelConverter.sequence(oldConnection, newConnection, error);
				}
				else if(newTable.equalsIgnoreCase("model_enzymatic_cofactor")) {
					positions.add(1);
					positions.add(2);
					positions.add(3);

					bits.add(3);

					genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
				}
				else if(newTable.equalsIgnoreCase("model_feature")) {
					positions.add(1);
					positions.add(2);
					positions.add(3);
					positions.add(5);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_sequence_feature")) {
					positions.add(1);
					positions.add(2);
					positions.add(4);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_pathway")) {
					positions.add(1);
					positions.add(2);
					positions.add(5);
					positions.add(3);
					positions.add(4);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_reaction")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_compartment")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_compartment")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_gene_has_compartment")) {
					positions.add(2);
					positions.add(1);
					positions.add(3);
					positions.add(4);

					bits.add(3);

					genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
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

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_compound")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_modules_has_compound")) {
					positions.add(2);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_aliases")) {
					positions.add(1);
					positions.add(4);
					positions.add(2);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_dictionary")) {
					positions.add(2);
					positions.add(1);
					positions.add(3);

					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
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

					bits.add(11);
					genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
				}
				else if(newTable.equalsIgnoreCase("model_dblinks")) {
					positions.add(1);
					positions.add(3);
					positions.add(2);
					positions.add(4);


					genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_stoichiometry")) {
					positions.add(1);
					positions.add(5);
					positions.add(4);
					positions.add(3);
					positions.add(2);

					ModelConverter.stoichiometry(this.oldConnection, this.newConnection, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_reaction_has_model_protein")) {
					//					positions.add(2);
					positions.add(3);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder("reaction_has_enzyme", newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_protein")) {
					ModelConverter.protein(this.oldConnection, this.newConnection, error);
				}
				else if(newTable.equalsIgnoreCase("model_pathway_has_model_protein")) {
					//					positions.add(2);
					positions.add(3);
					positions.add(1);

					genericDataRetrieverAndInjectionRespectingOrder("pathway_has_enzyme", newTable, positions, error);
				}
				else if(newTable.equalsIgnoreCase("model_subunit")) {
					ModelConverter.subunit(this.oldConnection, this.newConnection, error);
				}
				else if(newTable.equalsIgnoreCase("model_reaction")) {
					ModelConverter.reaction(this.oldConnection, this.newConnection, error);
				}
				else if(!newTable.equalsIgnoreCase("model_reaction_labels"))
					genericDataRetrieverAndInjection(oldTable, newTable, error);
				
				counter++;
				this.changes.firePropertyChange("tablesCounter", null, counter);

			}

		}
	}

	/**
	 * Convertion of model tables
	 * @throws InterruptedException 
	 */
	public void convertEnzymes() throws InterruptedException {

		for(String newTable : this.enzymesTables) {

			logger.info("Table: " + newTable);

			List<Integer> positions = new ArrayList<Integer>();
			List<Integer> bits = new ArrayList<Integer>();

			String oldTable = newTable.replace("enzymes_annotation_", "");
			
			int error = 0;

			if(newTable.equalsIgnoreCase("enzymes_annotation_organism")) {
				positions.add(1);
				positions.add(2);
				positions.add(4);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_productRank_has_organism")) {
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_ecNumberRank")) {
				positions.add(1);
				positions.add(3);
				positions.add(4);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_productRank")) {
				positions.add(1);
				positions.add(3);
				positions.add(4);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologues_has_ecNumber")) {
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_ecNumberList")) {
				positions.add(1);
				positions.add(3);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_productList")) {
				positions.add(1);
				positions.add(3);
				positions.add(2);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
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

				bits.add(8);
				bits.add(9);

				genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_homologyData")) {
				positions.add(1);
				positions.add(8);
				positions.add(6);
				positions.add(4);
				positions.add(3);
				positions.add(9);
				positions.add(5);
				positions.add(7);
				positions.add(2);

				bits.add(7);

				genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
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

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
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

				bits.add(8);

				genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bits, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_geneHomology_has_homologues")) {
				positions.add(1);
				positions.add(2);
				positions.add(6);
				positions.add(5);
				positions.add(4);
				positions.add(3);

				genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("enzymes_annotation_geneHomology")) {
				EnzymesConverter.geneHomology(this.oldConnection, this.newConnection, error);
			}
			else
				genericDataRetrieverAndInjection(oldTable, newTable, error);
			
			counter++;
			this.changes.firePropertyChange("tablesCounter", null, counter);

		}

	}

	/**
	 * Convertion of interpro tables
	 * @throws InterruptedException 
	 */
	public void convertInterpro() throws InterruptedException {

		for(String newTable : this.interproTables) {
			
			int error = 0;

			logger.info("Table: " + newTable);

			List<Integer> positions = new ArrayList<Integer>();

			if(newTable.equalsIgnoreCase("interpro_result_has_entry")){
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("interpro_result_has_model")){
				positions.add(2);
				positions.add(1);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("interpro_xRef")){
				positions.add(1);
				positions.add(2);
				positions.add(3);
				positions.add(5);
				positions.add(4);
				positions.add(6);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
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

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
			}
			else if(newTable.equalsIgnoreCase("interpro_results")){
				positions.add(1);
				positions.add(4);
				positions.add(5);
				positions.add(6);
				positions.add(2);
				positions.add(3);
				positions.add(7);

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
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

				genericDataRetrieverAndInjectionRespectingOrder(newTable, newTable, positions, error);
			}
			else
				genericDataRetrieverAndInjection(newTable, newTable, error);
			
			counter++;
			this.changes.firePropertyChange("tablesCounter", null, counter);

		}
	}

	/**
	 * @param oldTable
	 * @param newTable
	 * @throws InterruptedException 
	 */
	public void genericDataRetrieverAndInjection(String oldTable, String newTable, int error) throws InterruptedException {

		String query = "";

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			if(newConnection.getDatabase_type().equals(DatabaseType.H2))
				newTable = newTable.toLowerCase();

			newStatement.execute("DELETE FROM " + newTable + ";");

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM " + oldTable + ";");

			ResultSetMetaData rsmd = rs.getMetaData();

			int columns = rsmd.getColumnCount();

			while(rs.next()) {
				try {

					int col = 1;

					query = "INSERT INTO " + newTable + " VALUES (";

					while(col <= columns) {

						String data = null;

						if(rs.getString(col) != null)
							data = ModelConverter.str(rs.getString(col), newConnection.getDatabase_type());

						query +=  data ;

						if(col < columns)
							query += ", ";

						col++;
					}

					query += ");";

					newStatement.execute(query);

				} catch (JdbcSQLIntegrityConstraintViolationException e) {
					//		System.out.println("Primary key constraint violation in table " + newTable);
					//		e.printStackTrace();
				}
				catch (MySQLIntegrityConstraintViolationException e) {
					//		System.out.println("Primary key constraint violation in table " + newTable);
					//		e.printStackTrace();
				}
				catch (CommunicationsException e) {

					if(error < LIMIT) {
						
						logger.error("Communications exception! Retrying...");

						TimeUnit.MINUTES.sleep(1);

						error++;
						
						this.oldConnection = new Connection(this.oldConnection.getDatabaseAccess());
						this.newConnection = new Connection(this.newConnection.getDatabaseAccess());
								
						genericDataRetrieverAndInjection(oldTable, newTable, error);
					}
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
			}

			rs.close();

			oldStatement.close();
			newStatement.close();

		}
		catch (SQLException e) {

			if(oldTable.equalsIgnoreCase("protein_complex"))
				logger.warn("This database does not contain any protein_complex table!");
			else
				System.out.print(query);
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}

	/**
	 * @param oldTable
	 * @param newTable
	 * @throws InterruptedException 
	 */
	public void genericDataRetrieverAndInjectionRespectingOrder(String oldTable, String newTable, List<Integer> positions, int error) throws InterruptedException {

		try {
			Map<Integer, List<Integer>> alreadyUploaded = new HashMap<>();

			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			if(newConnection.getDatabase_type().equals(DatabaseType.H2))
				newTable = newTable.toLowerCase();

			newStatement.execute("DELETE FROM " + newTable + ";");

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM " + oldTable + ";");

			while(rs.next()) {

				try {
					String query = "INSERT INTO " + newTable + " VALUES (";

					int count = 1;

					for(int pos : positions) {

						String data = null;

						if(rs.getString(pos) != null)
							data = ModelConverter.str(rs.getString(pos), newConnection.getDatabase_type());

						query +=  data ;

						if(count < positions.size())
							query += ", ";

						count++;
					}

					query += ");";

					//						System.out.println(query);

					newStatement.execute(query);
				} catch (JdbcSQLIntegrityConstraintViolationException e) {
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//											e.printStackTrace();
				}
				catch (MySQLIntegrityConstraintViolationException e) {
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//											e.printStackTrace();
				}
				catch (CommunicationsException e) {

					if(error < LIMIT) {
						
						logger.error("Communications exception! Retrying...");

						TimeUnit.MINUTES.sleep(1);

						error++;
						
						this.oldConnection = new Connection(this.oldConnection.getDatabaseAccess());
						this.newConnection = new Connection(this.newConnection.getDatabaseAccess());
						
						genericDataRetrieverAndInjectionRespectingOrder(oldTable, newTable, positions, error);
					}
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
				//				}
			}

			rs.close();

			oldStatement.close();
			newStatement.close();
		} 
		catch (SQLException e) {

			System.out.println(newTable);

			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}

	/**
	 * @param oldTable
	 * @param newTable
	 * @throws InterruptedException 
	 */
	public void genericDataRetrieverAndInjectionRespectingOrderAndBitsType(String oldTable, String newTable, List<Integer> positions, List<Integer> bitsType, int error) throws InterruptedException {

		try {
			Statement oldStatement = oldConnection.createStatement();
			Statement newStatement = newConnection.createStatement();

			if(newConnection.getDatabase_type().equals(DatabaseType.H2))
				newTable = newTable.toLowerCase();

			newStatement.execute("DELETE FROM " + newTable + ";");

			ResultSet rs = oldStatement.executeQuery("SELECT * FROM " + oldTable + ";");

			while(rs.next()) {

				try {
					String query = "INSERT INTO " + newTable + " VALUES (";

					int count = 1;

					for(int pos : positions) {


						if(bitsType.contains(pos))
							query += rs.getInt(pos);
						else
							query += ModelConverter.str(rs.getString(pos), newConnection.getDatabase_type());

						if(count < positions.size())
							query += ", ";

						count++;
					}

					query += ");";

					newStatement.execute(query);
				} catch (JdbcSQLIntegrityConstraintViolationException e) {
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
				catch (MySQLIntegrityConstraintViolationException e) {
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
				catch (CommunicationsException e) {

					if(error < LIMIT) {
						
						logger.error("Communications exception! Retrying...");

						TimeUnit.MINUTES.sleep(1);

						error++;
						
						this.oldConnection = new Connection(this.oldConnection.getDatabaseAccess());
						this.newConnection = new Connection(this.newConnection.getDatabaseAccess());
						
						genericDataRetrieverAndInjectionRespectingOrderAndBitsType(oldTable, newTable, positions, bitsType, error);
					}
					//					System.out.println("Primary key constraint violation in table " + newTable);
					//					e.printStackTrace();
				}
			}

			rs.close();

			oldStatement.close();
			newStatement.close();
		} 
		catch (SQLException e) {

			System.out.println(newTable);

			Workbench.getInstance().error(e);
			e.printStackTrace();
		}
	}
	
	/**
	 * @param l
	 */
	public void addPropertyChangeListener(PropertyChangeListener l) {
		changes.addPropertyChangeListener(l);
	}

	/**
	 * @param l
	 */
	public void removePropertyChangeListener(PropertyChangeListener l) {
		changes.removePropertyChangeListener(l);
	}

	public void propertyChange(PropertyChangeEvent evt) {

		this.changes.firePropertyChange(evt.getPropertyName(), evt.getOldValue(), evt.getNewValue());				
	}

}
