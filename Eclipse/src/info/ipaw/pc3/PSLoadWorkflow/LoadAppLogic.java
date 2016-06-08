package info.ipaw.pc3.PSLoadWorkflow;

import java.sql.*;
import java.util.*;
import java.io.*;

/***
 * LoadAppLogic
 * 
 * @author yoges@microsoft.com
 * @author wesleiteixeira@id.uff.br
 */
public class LoadAppLogic 
{
	// ////////////////////////////////////////////////////////////
	// / Database Constants ///////////////////////////////////////
	// ////////////////////////////////////////////////////////////
	private static final String SQL_SERVER = "jdbc:derby:";
	private static final String SQL_USER = "IPAW";
	private static final String SQL_PASSWORD = "pc3_load-2009";

	// Initialize Apache Derby JDBC Driver
	private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	
	static 
	{
		try 
		{
			Class.forName(DERBY_DRIVER).newInstance();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	// ////////////////////////////////////////////////////////////
	// / Helper data structures ///////////////////////////////////
	// ////////////////////////////////////////////////////////////

	public static class CSVFileEntry 
	{
		public String FilePath;
		public String HeaderPath;
		public int RowCount;
		public String TargetTable;
		public String Checksum;
		public List<String> ColumnNames;
    
	    public String getFilePath() 
	    {
	    	return FilePath;
	    }
	    
	    public void setFilePath(String filePath) 
	    {
	    	FilePath = filePath;
	    }
	    
	    public String getHeaderPath() 
	    {
	    	return HeaderPath;
	    }
	    
	    public void setHeaderPath(String headerPath) 
	    {
	    	HeaderPath = headerPath;
	    }
	    
	    public int getRowCount()
		{
	    	return RowCount;
	    }
	    
	    public void setRowCount(int rowCount) 
	    {
	    	RowCount = rowCount;
	    }
	    
	    public String getTargetTable() 
	    {
	    	return TargetTable;
	    }
	    
	    public void setTargetTable(String targetTable) 
	    {
	    	TargetTable = targetTable;
	    }
	    
	    public String getChecksum() 
	    {
	    	return Checksum;
	    }
	    
	    public void setChecksum(String checksum) 
	    {
	    	Checksum = checksum;
	    }
	    
	    public List<String> getColumnNames() 
	    {
	    	return ColumnNames;
	    }
	    
	    public void setColumnNames(List<String> columnNames)
	    {
	    	ColumnNames = columnNames;
	    }
	}

	public static class DatabaseEntry 
	{
		public String DBGuid;
		public String DBName;
		public String ConnectionString;
    
		public String getDBGuid() 
		{
			return DBGuid;
		}
    
		public void setDBGuid(String guid)
		{
			DBGuid = guid;
		}
    
		public String getDBName() 
		{
			return DBName;
		}
    
		public void setDBName(String name)
		{
			DBName = name;
		}
    
		public String getConnectionString() 
		{
			return ConnectionString;
		}
    
		public void setConnectionString(String connectionString)
		{
			ConnectionString = connectionString;
		}
	}

	// ////////////////////////////////////////////////////////////
	// / Pre-Load Sanity Checks ///////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/***
	 * Checks if the CSV Ready File exists in the given rooth path to the CSV
	 * Batch
	 * 
	 * @param CSVRootPath
	 *            Path to the root directory for the batch
	 * @return true if the csv_ready.csv file exists in the CSVRoothPath. False
	 *         otherwise.
	 */
	public static boolean IsCSVReadyFileExists(String CSVRootPath) 
	{
		// 1. Check if parent directory exists.
		File RootDirInfo = new File(CSVRootPath);
		if (!RootDirInfo.exists()) return false;

		// 2. Check if CSV Ready file exists. We assume a static name for the
		// ready file.
		File ReadyFileInfo = new File(CSVRootPath, "csv_ready.csv");
		return ReadyFileInfo.exists();
	}

	/**
	 * @param CSVRootPath
	 * @return
	 */
	public static List<CSVFileEntry> ReadCSVReadyFile(String CSVRootPath) throws IOException 
	{
		// 1. Initialize output list of file entries
		List<CSVFileEntry> CSVFileEntryList = new ArrayList<CSVFileEntry>();

		// 2. Open input stream to read from CSV Ready File
		File CSVReadyFile = new File(CSVRootPath, "csv_ready.csv");
		BufferedReader ReadyFileStream = 
			new BufferedReader(new InputStreamReader(new FileInputStream(CSVReadyFile)));

		// 3. Read each line in CSV Ready file and split the lines into
		// individual columns separated by commas
		String ReadyFileLine;
		while ((ReadyFileLine = ReadyFileStream.readLine()) != null) 
		{
			// 3.a. Expect each line in the CSV ready file to be of the format:
			// <FileName>,<NumRows>,<TargetTable>,<MD5Checksum>
			String[] ReadyFileLineTokens = ReadyFileLine.split(",");

			// 3.b. Create an empty FileEntry and populate it with the columns
			CSVFileEntry FileEntry = new CSVFileEntry();
			FileEntry.FilePath = 
				CSVRootPath + File.separator + ReadyFileLineTokens[0].trim(); // column 1
			FileEntry.HeaderPath = FileEntry.FilePath + ".hdr";
			FileEntry.RowCount = 
				Integer.parseInt(ReadyFileLineTokens[1].trim()); // column 2
			FileEntry.TargetTable = ReadyFileLineTokens[2].trim(); // column 3
			FileEntry.Checksum = ReadyFileLineTokens[3].trim(); // column 4

			// 3.c. Add file entry to output list
			CSVFileEntryList.add(FileEntry);
		}

		// 4. Close input stream and return output file entry list
		ReadyFileStream.close();
		return CSVFileEntryList;
	}

	/**
	 * Check if the correct list of files/table names are present in this
	 * 
	 * @param FileEntries
	 * @return
	 */
	public static boolean IsMatchCSVFileTables(List<CSVFileEntry> FileEntries) 
	{
		// check if the file count and the expected number of tables match
		if (LoadConstants.EXPECTED_TABLES.size() != FileEntries.size()) return false;

		// for each expected table name, check if it is present in the list of
		// file entries
		for (String TableName : LoadConstants.EXPECTED_TABLES) 
		{
			boolean TableExists = false;
			for (CSVFileEntry FileEntry : FileEntries) 
			{
				if (!TableName.equals(FileEntry.TargetTable)) continue;
				TableExists = true; // found a match
				break;
			}
			// if the table name did not exist in list of CSV files, this check
			// fails.
			if (!TableExists) return false;
		}

		return true;
	}

	/**
	 * Test if a CSV File defined in the CSV Ready list actually exists on disk.
	 * 
	 * @param FileEntry
	 *            FileEntry for CSVFile to test
	 * @return True if the FilePath in the given FileEntry exists on disk. False
	 *         otherwise.
	 */
	public static boolean IsExistsCSVFile(CSVFileEntry FileEntry)
	{
		File CSVFileInfo = new File(FileEntry.FilePath);
		if(!CSVFileInfo.exists()) return false;
		File CSVFileHeaderInfo = new File(FileEntry.HeaderPath);
		return CSVFileHeaderInfo.exists();
	}

	/**
	 * @param FileEntry
	 * @return
	 * @throws Exception
	 */
	public static CSVFileEntry ReadCSVFileColumnNames(CSVFileEntry FileEntry) throws Exception 
	{
		// 2. Read the header line of the CSV File.
		BufferedReader CSVFileReader = 
			new BufferedReader(new InputStreamReader(new FileInputStream(FileEntry.HeaderPath)));
		String HeaderRow = CSVFileReader.readLine();

		// 3. Extract the comma-separated columns names of the CSV File from its
		// header line.
		// Strip empty spaces around column names.
		String[] ColumnNames = HeaderRow.split(",");
		FileEntry.ColumnNames = new ArrayList<String>();
		for (String ColumnName : ColumnNames) FileEntry.ColumnNames.add(ColumnName.trim());

		CSVFileReader.close();

		return FileEntry;
	}

	/**
	 * Checks if the correct list of column headers is present for the CSV file
	 * to match the table
	 * 
	 * @param FileEntry
	 *            FileEntry for CSV File whose column headers to test
	 * @return True if the column headers present in the CSV File are the same
	 *         as the expected table columns. False otherwise.
	 */
	public static boolean IsMatchCSVFileColumnNames(CSVFileEntry FileEntry) 
	{
		// determine expected columns
		List<String> ExpectedColumns = null;
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) 
		{
			ExpectedColumns = LoadConstants.EXPECTED_DETECTION_COLS;
		} 
		else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable))
		{
			ExpectedColumns = LoadConstants.EXPECTED_FRAME_META_COLS;
		} 
		else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable))
		{
			ExpectedColumns = LoadConstants.EXPECTED_IMAGE_META_COLS;
		} 
		else 
		{
			// none of the table types match...invalid
			return false;
		}

		// test if the expected and present column name counts are the same
		if (ExpectedColumns.size() != FileEntry.ColumnNames.size()) return false;

		// test of all expected names exist in the columns present
		for (String ColumnName : ExpectedColumns) 
		{
			if (!FileEntry.ColumnNames.contains(ColumnName)) return false; // mismatch
		}

		// all columns match
		return true;
	}

	// ////////////////////////////////////////////////////////////
	// / Loading Section //////////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/**
	 * @param JobID
	 * @return
	 * @throws Exception
	 */
	public static DatabaseEntry CreateEmptyLoadDB(String JobID) throws Exception 
	{
		// initialize database entry for storing database properties
		DatabaseEntry DBEntry = new DatabaseEntry();
		DBEntry.DBName = JobID + "_" + System.currentTimeMillis() + "_LoadDB";
		DBEntry.DBGuid = UUID.randomUUID().toString();

		// initialize Sql Connection String to sql server
		StringBuilder ConnStr = new StringBuilder(SQL_SERVER);
		ConnStr.append(";databaseName=");
		ConnStr.append(DBEntry.DBName);
		ConnStr.append(";user=");
		ConnStr.append(SQL_USER);
		ConnStr.append(";password=");
		ConnStr.append(SQL_PASSWORD);

		// Create empty database instance
		Connection SqlConn = null;
		try 
		{
			String CreateDBConnStr = ConnStr.toString() + ";create=true";
			SqlConn = DriverManager.getConnection(CreateDBConnStr);
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}

		// update Sql Connection String to new create tables
		DBEntry.ConnectionString = ConnStr.toString();

		// create tables
		SqlConn = null;
		try 
		{
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// Create P2 Table
			Statement SqlCmd = SqlConn.createStatement();
			SqlCmd.executeUpdate(LoadSql.CREATE_DETECTION_TABLE);
			// Create P2FrameMeta Table
			SqlCmd.executeUpdate(LoadSql.CREATE_FRAME_META_TABLE);
			// Create P2ImageMeta Table
			SqlCmd.executeUpdate(LoadSql.CREATE_IMAGE_META_TABLE);

		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}

		return DBEntry;
	}

	/**
	 * Loads a CSV File into an existing table using derby bulk load:
	 * SYSCS_UTIL.SYSCS_IMPORT_TABLE
	 * 
	 * @param DBEntry
	 *            Database into which to load the CSV file
	 * @param FileEntry
	 *            File to be bulk loaded into database table
	 * @return True if the bulk load ran without exceptions. False otherwise.
	 */
	public static boolean LoadCSVFileIntoTable(DatabaseEntry DBEntry, CSVFileEntry FileEntry) throws Exception 
	{
		Connection SqlConn = null;
		try 
		{
			// connect to database instance
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// build bulk insert SQL command
			CallableStatement SqlCmd = 
				SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?,?,?,?,?,?,?)");

			SqlCmd.setString(1, null);
			SqlCmd.setString(2, FileEntry.TargetTable.toUpperCase());
			SqlCmd.setString(3, FileEntry.FilePath);
			SqlCmd.setString(4, ",");
			SqlCmd.setString(5, null);
			SqlCmd.setString(6, null);
			SqlCmd.setShort(7, (short)0);

			// execute bulk insert command
			SqlCmd.execute();

		} 
		catch (Exception ex) 
		{
			// bulk insert failed
			return false;
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}

		// bulk insert success
		return true;
	}

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean UpdateComputedColumns(DatabaseEntry DBEntry, CSVFileEntry FileEntry) throws Exception 
	{
		Connection SqlConn = null;
		try 
		{
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) 
			{
				// Update ZoneID
				Statement SqlCmd = SqlConn.createStatement();
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET zoneID = (\"dec\"+(90.0))/(0.0083333)");

				// Update cx
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cx = (COS(RADIANS(\"dec\"))*COS(RADIANS(ra)))");

				// Update cy
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cy = COS(RADIANS(\"dec\"))*SIN(RADIANS(ra))");

				// Update cz
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cz = (SIN(RADIANS(\"dec\")))");
				
			} 
			else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) 
			{
				// No columns to be updated for FrameMeta
			} 
			else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) 
			{
				// No columns to be updated for ImageMeta
			} 
			else 
			{
				// none of the table types matches...invalid
				return false;
			}

		} 
		catch (Exception ex) 
		{
			// update column failed
			return false;
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}
		// update column success
		return true;
	}

	// ////////////////////////////////////////////////////////////
	// / Post-Load Checks /////////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean IsMatchTableRowCount(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception 
	{
		// does the number of rows expected match the number of rows loaded
		Connection SqlConn = null;
		try 
		{
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			Statement SqlCmd = SqlConn.createStatement();
			// execute row count command
			ResultSet Results = 
				SqlCmd.executeQuery("SELECT COUNT(*) FROM " + FileEntry.TargetTable);
			if (Results.next()) 
			{
				// check if row count matches expected row count
				int RowCount = (int) Results.getInt(1);
				return RowCount == FileEntry.RowCount;
			} 
			else 
			{
				return false; // error case!
			}
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}
	}

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean IsMatchTableColumnRanges(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception 
	{

		// determine expected column ranges
		List<LoadConstants.ColumnRange> ExpectedColumnRanges = null;
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) 
		{
			ExpectedColumnRanges = LoadConstants.EXPECTED_DETECTION_COL_RANGES;
		} 
		else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable))
		{
			// No columns range values available for FrameMeta
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} 
		else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) 
		{
			// No columns range values available for ImageMeta
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} 
		else 
		{
			// none of the table types matches...invalid
			return false;
		}

		// connect to database instance
		Connection SqlConn = null;
		try 
		{
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// For each column in available list, test if rows in table fall
			// outside expected range
			for (LoadConstants.ColumnRange Column : ExpectedColumnRanges)
			{
				// build SQL command for error count
				String SqlStr = 
					String.format(
							"SELECT COUNT(*) FROM %1$s WHERE (%2$s < %3$s OR %2$s > %4$s) AND %2$s != -999",
							FileEntry.TargetTable, Column.ColumnName,
							Column.MinValue, Column.MaxValue);

				// execute range error count command
				Statement SqlCmd = SqlConn.createStatement();
				ResultSet Results = SqlCmd.executeQuery(SqlStr);
				if (Results.next()) 
				{
					int ErrorCount = Results.getInt(1);
					if (ErrorCount > 0) return false; // found a range error
				} 
				else 
				{
					return false; // error case
				}
			}
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}

		return true; // no range errors found
	}

	/**
	 * @param DBEntry
	 */
	public static void CompactDatabase(DatabaseEntry DBEntry) throws Exception 
	{
		// Shrink database instance
		Connection SqlConn = null;
		try 
		{
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			for (String TableName : LoadConstants.EXPECTED_TABLES) {
				CallableStatement SqlCmd = 
					SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
				SqlCmd.setString(1, SQL_USER.toUpperCase());
				SqlCmd.setString(2, TableName.toUpperCase());
				SqlCmd.setShort(3, (short) 1);
				SqlCmd.setShort(4, (short) 1);
				SqlCmd.setShort(5, (short) 1);
				SqlCmd.execute();
			}
		} 
		finally 
		{
			if (SqlConn != null) SqlConn.close();
		}
	}
}
