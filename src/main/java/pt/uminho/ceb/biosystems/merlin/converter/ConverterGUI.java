package pt.uminho.ceb.biosystems.merlin.converter;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.uvigo.ei.aibench.core.operation.annotation.Cancel;
import es.uvigo.ei.aibench.core.operation.annotation.Direction;
import es.uvigo.ei.aibench.core.operation.annotation.Operation;
import es.uvigo.ei.aibench.core.operation.annotation.Port;
import es.uvigo.ei.aibench.core.operation.annotation.Progress;
import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.aibench.gui.CustomGUI;
import pt.uminho.ceb.biosystems.merlin.aibench.utilities.TimeLeftProgress;
import pt.uminho.ceb.biosystems.merlin.bioapis.externalAPI.utilities.Enumerators.FileExtensions;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Connection;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.H2DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.services.DatabaseServices;
import pt.uminho.ceb.biosystems.merlin.utilities.io.ConfFileReader;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;

@Operation(name="convert workspace to merlin 4", description="converts a given workspace from merlin 3 to merlin 4")
public class ConverterGUI implements PropertyChangeListener {

	public static final int DEFAULT_RATIO = 5;
	private String oldWorkspaceName;
	private String newWorkspaceName;
	private String merlinDirectoryPath;
	private Integer dataSize = 1;
	private long startTime;
	private String message;
	private TimeLeftProgress progress = new TimeLeftProgress();
	private static String today = setToday();
	private boolean override = false;
	private Merlin3ToMerlin4 converter;
	private boolean go = false;

	private static final Logger logger = LoggerFactory.getLogger(ConverterGUI.class);

	@Port(direction=Direction.INPUT, name="new workspace name", description="write the new workspace's name", validateMethod = "checkIfValidName", order=2)
	public void setNewWorkspaceName(String newWorkspaceName) {

		try {
			List<String> names = DatabaseServices.getDatabasesAvailable();

			if(names.contains(newWorkspaceName) && !override)
				throw new Exception("workspace name already in use, please select a different name! "
						+ "If you wish to override an existing database, please select 'force database creation' at this operations' menu.");
			else
				this.newWorkspaceName = newWorkspaceName;

			this.go = true;
		} 
		catch (Exception e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}

	}

	@Port(direction=Direction.INPUT, name="old workspace name", description="write the new workspace's name", validateMethod = "checkIfValidName", order=3)
	public void setOldWorkspaceName(String oldWorkspaceName) {

		this.oldWorkspaceName = oldWorkspaceName;

	}

	@Port(direction=Direction.INPUT, name="merlin 3 home directory", description="select the home directory of merlin 3", order=4)
	public void setNewProject(File merlinDirectory) {

		if(this.go) {

			try {

				this.startTime = GregorianCalendar.getInstance().getTimeInMillis();

				this.progress.setTime((GregorianCalendar.getInstance().getTimeInMillis() - startTime), 0, this.dataSize, "initializing new database in merlin 4");

				DatabaseServices.generateDatabase(this.newWorkspaceName);

				DatabaseServices.dropConnection(this.newWorkspaceName);

				this.merlinDirectoryPath = merlinDirectory.getAbsolutePath().concat("/");

				DatabaseAccess newDBAccess = generateNewDatabaseDBAccess();
				DatabaseAccess oldDBAccess = generateOldDatabaseDBAccess();

				Connection newConnection = new Connection(newDBAccess);
				Connection oldConnection = new Connection(oldDBAccess);

				this.converter = new Merlin3ToMerlin4(oldConnection, newConnection);

				this.converter.addPropertyChangeListener(this);

				this.converter.start();

				oldConnection.closeConnection();
				newConnection.closeConnection();

				this.startTime = GregorianCalendar.getInstance().getTimeInMillis();

				this.dataSize = 3;
				this.message = "importing workspace files and configurations";

				this.executeChange(0);

				Long taxId = Long.valueOf(this.converter.getTaxId());

				this.executeChange(1);

				importWorkspaceFiles();

				this.executeChange(2);

				FileExtensions extension = checkIfGenomeLoaded(taxId);

				if(extension != null)
					updateNewLogFile(this.newWorkspaceName, taxId, extension);

				this.executeChange(3);

				Workbench.getInstance().info("workspace successfully imported!");
			} 
			catch (IOException e) {
				Workbench.getInstance().warn("All data was successfully converted, however the workspace folder in merlin 3 was not found!");
				e.printStackTrace();
			}
			catch (Exception e) {
				Workbench.getInstance().error("An error occurred while converting the workspace!");
				e.printStackTrace();
			}
		}

	}

	@Port(direction=Direction.INPUT, name="force database creation", description="this command forces merlin to create a database with the seleted name. If a database with such name already exists, it will be replaced",
			advanced = true, defaultValue = "false", order=1)
	public void setOldWorkspaceName(boolean override) {

		this.override = override;

	}

	private FileExtensions checkIfGenomeLoaded(long taxId) {

		File workspace = new File(FileUtils.getWorkspaceTaxonomyFolderPath(this.newWorkspaceName, taxId));

		File[] files = workspace.listFiles();

		for(File file : files) {

			if(file.getName().equals(FileExtensions.PROTEIN_FAA.getName()))
				return FileExtensions.PROTEIN_FAA;
			else if(file.getName().equals(FileExtensions.GENOMIC_FNA.getName()))
				return FileExtensions.GENOMIC_FNA;
		}

		return null;
	}

	/**
	 * @param databaseName
	 * @param taxID
	 * @param extension
	 * @throws IOException
	 */
	private static void updateNewLogFile(String databaseName, Long taxID, FileExtensions extension) throws IOException{

		String tempPath = FileUtils.getWorkspacesFolderPath();
		StringBuffer buffer = new StringBuffer();
		if(new File(tempPath+"genomes.log").exists()) {

			FileInputStream finstream = new FileInputStream(tempPath+"genomes.log");
			DataInputStream in = new DataInputStream(finstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String strLine;
			while ((strLine = br.readLine()) != null) {

				buffer.append(strLine+"\n");
			}
			br.close();
			in.close();
			finstream.close();
		}
		else
		{
			buffer.append("organismID\tdate\n");
		}

		new File(tempPath+"genomes.log");
		FileWriter fstream = new FileWriter(tempPath+"genomes.log");  
		BufferedWriter out = new BufferedWriter(fstream);
		out.append(buffer);
		out.write("genome_"+databaseName+"_"+taxID+"_"+extension.getName()+"\t"+today);
		out.close();
	}

	//////////////////////////ValidateMethods/////////////////////////////

	public void checkIfValidName(String name) throws Exception {

		try {
			if(name == null || name.isEmpty())
				throw new Exception("please insert a valid name");
		} 
		catch (Exception e) {
			Workbench.getInstance().error(e);
			e.printStackTrace();
		}

	}

	private void importWorkspaceFiles() throws IOException {

		File newWorkspace = new File(FileUtils.getWorkspaceFolderPath(this.newWorkspaceName));

		newWorkspace.mkdirs();

		File oldWorkspace = new File(this.merlinDirectoryPath + "/ws/" + this.oldWorkspaceName);

		org.apache.commons.io.FileUtils.copyDirectory(oldWorkspace, newWorkspace);

	}

	/**
	 * @return
	 * @throws IOException
	 */
	private DatabaseAccess generateNewDatabaseDBAccess() throws IOException{
		Map<String, String> settings = ConfFileReader.loadConf(FileUtils.getConfFolderPath()+"/database_settings.conf");
		DatabaseType dbType = DatabaseType.MYSQL;


		String userName = settings.get("username");
		String password = settings.get("password");
		String host = settings.get("host");
		String port = settings.get("port");

		if(settings.get("dbtype").equals("h2")) {
			dbType = DatabaseType.H2;
			userName = settings.get("h2_username");
			password = settings.get("h2_password");
		}


		return generateDBAccess(host, this.newWorkspaceName, password, port, userName, dbType);
	}

	/**
	 * @return
	 * @throws IOException
	 */
	private DatabaseAccess generateOldDatabaseDBAccess() throws IOException{

		Map<String, String> settings = ConfFileReader.loadConf(this.merlinDirectoryPath+"conf/database_settings.conf");
		DatabaseType dbType = DatabaseType.MYSQL;

		String userName = settings.get("username");
		String password = settings.get("password");
		String host = settings.get("host");
		String port = settings.get("port");

		if(settings.get("dbtype").equals("h2")) {
			dbType = DatabaseType.H2;
			userName = settings.get("h2_username");
			password = settings.get("h2_password");
			host = this.merlinDirectoryPath;
		}

		return generateDBAccess(host, this.oldWorkspaceName, password, port, userName, dbType);
	}

	/**
	 * @param host
	 * @param databaseName
	 * @param password
	 * @param port
	 * @param username
	 * @param type
	 * @return
	 */
	public static DatabaseAccess generateDBAccess(String host, String databaseName, String password, String port,
			String username, DatabaseType type) {

		if(type.equals(DatabaseType.MYSQL))
			return new MySQLDatabaseAccess(username, password, host, port, databaseName);
		else
			return new H2DatabaseAccess(username, password, databaseName, host);
	}

	/**
	 * @return
	 */
	private static String setToday() {

		Calendar currentDate = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MMM/dd");
		return formatter.format(currentDate.getTime());
	}

	/**
	 * @return the progress
	 */
	@Progress
	public TimeLeftProgress getProgress() {

		return progress;
	}

	/**
	 * 
	 */
	@Cancel
	public void cancel() {

		String[] options = new String[2];
		options[0]="yes";
		options[1]="no";

		int result=CustomGUI.stopQuestion("cancel confirmation", "are you sure you want to cancel the operation?", options);

		if(result==0) {

			progress.setTime((GregorianCalendar.getInstance().getTimeInMillis()-GregorianCalendar.getInstance().getTimeInMillis()),1,1);
			DatabaseServices.setCancelExporterBatch(this.newWorkspaceName, true);
			logger.warn("export workspace operation canceled!");
			Workbench.getInstance().warn("Please hold on. Your operation is being cancelled.");
		}
	}

	@Override
	public void propertyChange(PropertyChangeEvent evt) {

		if(evt.getPropertyName().equalsIgnoreCase("message"))
			this.message = (String) evt.getNewValue();

		if(evt.getPropertyName().equalsIgnoreCase("size")) {
			this.dataSize = (int) evt.getNewValue();
		}

		if(evt.getPropertyName().equalsIgnoreCase("tablesCounter")) {

			int counter = (int) evt.getNewValue();
			executeChange(counter);
		}
	}

	public void executeChange(int counter) {

		this.progress.setTime((GregorianCalendar.getInstance().getTimeInMillis() - startTime), counter, this.dataSize, this.message);
	}

}



