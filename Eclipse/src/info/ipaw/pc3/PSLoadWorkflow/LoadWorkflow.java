package info.ipaw.pc3.PSLoadWorkflow;

import java.util.*;


public class LoadWorkflow {

  public static void main(String[] args) throws Exception {
    
    String JobID = args[0], CSVRootPath = args[1];

    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    // 2. Control Flow: Decision
    if (!IsCSVReadyFileExistsOutput) throw new RuntimeException("IsCSVReadyFileExists failed");


    // 3. ReadCSVReadyFile
    List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);


    // 4. IsMatchCSVFileTables
    boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    // 5. Control Flow: Decision
    if (!IsMatchCSVFileTablesOutput) throw new RuntimeException("IsMatchCSVFileTables failed");


    // 6. CreateEmptyLoadDB
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);


    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput) {

      // ///////////////////////////////////
      // //// Pre Load Validation //////
      // ///////////////////////////////////
      // 7.a. IsExistsCSVFile
      boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
      // 7.b. Control Flow: Decision
      if (!IsExistsCSVFileOutput) throw new RuntimeException("IsExistsCSVFile failed");


      // 7.c. ReadCSVFileColumnNames
      LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
          LoadAppLogic.ReadCSVFileColumnNames(FileEntry);


      // 7.d. IsMatchCSVFileColumnNames
      boolean IsMatchCSVFileColumnNamesOutput =
          LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
      // 7.e. Control Flow: Decision
      if (!IsMatchCSVFileColumnNamesOutput)
        throw new RuntimeException("IsMatchCSVFileColumnNames failed");


      // ///////////////////////////////////
      // //// Load File //////
      // ///////////////////////////////////
      // 7.f. LoadCSVFileIntoTable
      boolean LoadCSVFileIntoTableOutput =
          LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
      // 7.g. Control Flow: Decision
      if (!LoadCSVFileIntoTableOutput) throw new RuntimeException("LoadCSVFileIntoTable failed");


      // 7.h. UpdateComputedColumns
      boolean UpdateComputedColumnsOutput =
          LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
      // 7.i. Control Flow: Decision
      if (!UpdateComputedColumnsOutput) throw new RuntimeException("UpdateComputedColumns failed");


      // ///////////////////////////////////
      // //// PostLoad Validation //////
      // ///////////////////////////////////
      // 7.j. IsMatchTableRowCount
      boolean IsMatchTableRowCountOutput =
          LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
      // 7.k. Control Flow: Decision
      if (!IsMatchTableRowCountOutput) throw new RuntimeException("IsMatchTableRowCount failed");


      // 7.l. IsMatchTableColumnRanges
      boolean IsMatchTableColumnRangesOutput =
          LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
              ReadCSVFileColumnNamesOutput);
      // 7.m. Control Flow: Decision
      if (!IsMatchTableColumnRangesOutput)
        throw new RuntimeException("IsMatchTableColumnRanges failed");

    }


    // 8. CompactDatabase
    LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);
  }
}
