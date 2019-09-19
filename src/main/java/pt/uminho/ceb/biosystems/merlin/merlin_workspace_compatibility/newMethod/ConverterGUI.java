package pt.uminho.ceb.biosystems.merlin.merlin_workspace_compatibility.newMethod;

import java.io.File;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import es.uvigo.ei.aibench.core.operation.annotation.Cancel;
import es.uvigo.ei.aibench.core.operation.annotation.Direction;
import es.uvigo.ei.aibench.core.operation.annotation.Operation;
import es.uvigo.ei.aibench.core.operation.annotation.Port;
import es.uvigo.ei.aibench.core.operation.annotation.Progress;
import es.uvigo.ei.aibench.workbench.Workbench;
import pt.uminho.ceb.biosystems.merlin.aibench.datatypes.WorkspaceAIB;
import pt.uminho.ceb.biosystems.merlin.aibench.utilities.TimeLeftProgress;
import pt.uminho.ceb.biosystems.merlin.dataAccess.IDatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.dataAccess.InitDataAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.H2DatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.MySQLDatabaseAccess;
import pt.uminho.ceb.biosystems.merlin.utilities.io.ConfFileReader;
import pt.uminho.ceb.biosystems.merlin.utilities.io.FileUtils;
import pt.uminho.ceb.biosystems.merlin.database.connector.datatypes.Enumerators.DatabaseType;

@Operation(name="convert workspace to merlin 4", description="converts a given workspace from merlin 3 to merlin 4")
public class ConverterGUI {
	
		public static final int DEFAULT_RATIO = 5;
		private WorkspaceAIB workspace;
		private String oldWorkspaceName;
		private String newWorkspaceName;
		private String merlinDirectoryPath;
		private long startTime;
		private String message;
		private TimeLeftProgress progress = new TimeLeftProgress();
		private AtomicBoolean cancel = new AtomicBoolean(false);
		private AtomicInteger querySize;
		private AtomicInteger counter = new AtomicInteger(0);
		
		@Port(direction=Direction.INPUT, name="new workspace name", description="write the new workspace's name", order=1)
		public void setNewWorkspaceName(String newWorkspaceName) {

			try {
				List<String> names = InitDataAccess.getInstance().getDatabasesAvailable();
				
				if(names.contains(newWorkspaceName))
					throw new Exception("workspace name already in use, please select a different name!");
				else
					this.newWorkspaceName = newWorkspaceName;
				
			} 
			catch (Exception e) {
				Workbench.getInstance().error(e);
				e.printStackTrace();
			}
			
		}
		
		@Port(direction=Direction.INPUT, name="old workspace name", description="write the new workspace's name", order=2)
		public void setOldWorkspaceName(String oldWorkspaceName) {

			this.oldWorkspaceName = oldWorkspaceName;

		}
		
		@Port(direction=Direction.INPUT, name="merlin 3 home directory", description="select the home directory of merlin 3", order=3)
		public void setNewProject(File merlinDirectory) {

			this.merlinDirectoryPath = merlinDirectory.getAbsolutePath();
			
		}
		
		//////////////////////////ValidateMethods/////////////////////////////
		/**
		 * @param project
		 */
		public void checkWorkspace(WorkspaceAIB workspace) {

//			if(workspace == null) {
//
//				throw new IllegalArgumentException("no workspace selected!");
//			}
//			else {
//
//				this.workspace = workspace;
//				this.homologyDataContainer = (AnnotationEnzymesAIB) AIBenchUtils.getEntity(this.workspace.getName(), AnnotationEnzymesAIB.class);
//				
//				if(homologyDataContainer == null)
//					throw new IllegalArgumentException("please open the enzymes annotation view before generating a new sample!");
//			}
		}
		
		public void checkUniqueWorkspace(WorkspaceAIB workspace) {
		}
		
	    private IDatabaseAccess readDatabaseConfigurations() throws IOException{
	    	Map<String, String> settings = ConfFileReader.loadConf(FileUtils.getConfFolderPath()+"/database_settings.conf");
			String dbType = settings.get("dbtype");
			
			
			
//			if(dbType.equals("mysql")) 
//				return new MySQLDatabaseAccess(settings.get("username"), settings.get("password"), settings.get("host"), settings.get("port"));
//			else if(dbType.equals("h2"))
//				return new H2DatabaseAccess(settings.get("h2_username"), settings.get("h2_password"));
			return null;
	    }
		
		public static DatabaseAccess generateDBAccess(String host, String databaseName, String password, String port,
				String username, DatabaseType type) {
			
			if(type.equals(DatabaseType.MYSQL))
				return new MySQLDatabaseAccess(username, password, host, port, databaseName);
			else
				return new H2DatabaseAccess(username, password, databaseName, host);
		}
		
		/**
		 * @return the progress
		 */
		@Progress
		public TimeLeftProgress getProgress() {

			return progress;
		}

		/**
		 * @param cancel the cancel to set
		 */
		@Cancel
		public void setCancel() {

			progress.setTime(0, 0, 0);
			Workbench.getInstance().warn("operation canceled!");
			this.cancel.set(true);
		}
		
	}


	
