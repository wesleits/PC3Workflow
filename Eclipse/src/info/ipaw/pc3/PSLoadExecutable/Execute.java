package info.ipaw.pc3.PSLoadExecutable;

import info.ipaw.pc3.PSLoadWorkflow.LoadAppLogic;
import info.ipaw.pc3.PSLoadWorkflow.LoadAppLogic.*;

import java.beans.*;
import java.io.*;
import java.util.*;


public class Execute {
  public static String USAGE =
    new StringBuilder()
  .append("USAGE:\n") 
  .append("java PSLoadExecutable.Execute <ActivityName> [-x '<XmlSerializedInputParam_1>']")
  .append(" [-x '<XmlSerializedInputParam_2>'] ...\n")
  .append("java PSLoadExecutable.Execute <ActivityName> -o <FilePathToXmlSerializedOutputParam_1>")
  .append(" [-f <FilePathToXmlSerializedParam_1>] [-f <FilePathToXmlSerializedInputParam_2>] ...\n")
  .toString();

  private static String ITER_CHAR = "?";

  public static void main (String[] args) throws Exception {
    if (args == null || args.length == 0) throw new RuntimeException(USAGE);

    int Index = 0;
    // method to call
    String ActivityName = args[Index++];

    // write to console or file?
    OutputStream OutputWriter = null;
    String OutputFileName = null;
    if (Index < args.length && "-o".equalsIgnoreCase(args[Index])) {
      Index++;
      OutputFileName = args[Index++];
      if(!OutputFileName.contains(ITER_CHAR)) OutputWriter = new FileOutputStream(OutputFileName);
    } else {
      OutputWriter = System.out;
    }

    String[] SerializedParams = new String[args.length - Index];
    SerializedParams = Arrays.copyOfRange(args, Index, args.length);

    Object Output = null;
    Object[] Params = ParseParams(SerializedParams);
    if(ActivityName.equalsIgnoreCase("IsCSVReadyFileExists")) {          
      String CSVRootPathInput = (String)Params[0];
      Output = LoadAppLogic.IsCSVReadyFileExists(CSVRootPathInput);
    } else
      if(ActivityName.equalsIgnoreCase("ReadCSVReadyFile")) {
        String CSVRootPathInput = (String)Params[0];
        Output = LoadAppLogic.ReadCSVReadyFile(CSVRootPathInput);          
      }
      else
        if(ActivityName.equalsIgnoreCase("IsMatchCSVFileTables")) {
          List<CSVFileEntry> CSVFileEntries = (List<CSVFileEntry>) Params[0];
          Output = LoadAppLogic.IsMatchCSVFileTables(CSVFileEntries);
        }
        else
          if(ActivityName.equalsIgnoreCase("CreateEmptyLoadDB")) {
            String JobIDInput = (String) Params[0];
            Output = LoadAppLogic.CreateEmptyLoadDB(JobIDInput);
          }
          else
            if(ActivityName.equalsIgnoreCase("SplitList")) {
              List<CSVFileEntry> CSVFileEntries = (List<CSVFileEntry>) Params[0];
              int FileCount = 0;
              for (CSVFileEntry Entry : CSVFileEntries) {
                if (OutputFileName != null) {
                  OutputWriter = 
                    new FileOutputStream(OutputFileName.replace(ITER_CHAR, "" + FileCount++));
                  OutputWriter.write(SerializeParam(Entry).getBytes());
                  OutputWriter.close();
                } else {
                  System.out.println(SerializeParam(Entry));            
                }
              }
              return;
            }
            else
              if(ActivityName.equalsIgnoreCase("IsExistsCSVFile")) {
                CSVFileEntry FileEntry = (CSVFileEntry)Params[0];
                Output = LoadAppLogic.IsExistsCSVFile(FileEntry);        
              }
              else
                if(ActivityName.equalsIgnoreCase("ReadCSVFileColumnNames")) {
                  CSVFileEntry FileEntry = (CSVFileEntry) Params[0];
                  Output = LoadAppLogic.ReadCSVFileColumnNames(FileEntry);
                }
                else
                  if(ActivityName.equalsIgnoreCase("IsMatchCSVFileColumnNames")) {
                    CSVFileEntry FileEntry = (CSVFileEntry) Params[0];
                    Output = LoadAppLogic.IsMatchCSVFileColumnNames(FileEntry);
                  }
                  else
                    if(ActivityName.equalsIgnoreCase("LoadCSVFileIntoTable")) {
                      DatabaseEntry DBEntry = (DatabaseEntry) Params[0];
                      CSVFileEntry FileEntry = (CSVFileEntry) Params[1];
                      Output = LoadAppLogic.LoadCSVFileIntoTable(DBEntry, FileEntry);
                    }
                    else
                      if(ActivityName.equalsIgnoreCase("UpdateComputedColumns")) {
                        DatabaseEntry DBEntry = (DatabaseEntry) Params[0];
                        CSVFileEntry FileEntry = (CSVFileEntry) Params[1];
                        Output = LoadAppLogic.UpdateComputedColumns(DBEntry, FileEntry);
                      }
                      else
                        if(ActivityName.equalsIgnoreCase("IsMatchTableRowCount")) {
                          DatabaseEntry DBEntry = (DatabaseEntry) Params[0];
                          CSVFileEntry FileEntry = (CSVFileEntry) Params[1];
                          Output = LoadAppLogic.IsMatchTableRowCount(DBEntry, FileEntry);
                        }
                        else
                          if(ActivityName.equalsIgnoreCase("IsMatchTableColumnRanges")) {
                            DatabaseEntry DBEntry = (DatabaseEntry) Params[0];
                            CSVFileEntry FileEntry = (CSVFileEntry) Params[1];
                            Output = LoadAppLogic.IsMatchTableColumnRanges(DBEntry, FileEntry);
                          }
                          else
                            if(ActivityName.equalsIgnoreCase("CompactDatabase")) {
                              DatabaseEntry DBEntry = (DatabaseEntry) Params[0];
                              LoadAppLogic.CompactDatabase(DBEntry);
                            }
                            else {
                              System.out.println("Activity not found! " + ActivityName);
                            }

    if (Output != null) {
      OutputWriter.write(SerializeParam(Output).getBytes());
      OutputWriter.close();
    }
  }

  /**
   * 
   * @param Params
   * @return
   * @throws IOException 
   * @throws IOException 
   */
  public static Object[] ParseParams(String[] Params) throws IOException {
    List<Object> OutputParams = new ArrayList<Object>();
    if (Params == null || Params.length == 0) return OutputParams.toArray();
    if (Params.length % 2 != 0) throw new RuntimeException("Odd number of args.");

    int Index = 0;      
    while(Index < Params.length) {
      String SerializedParam;
      if("-f".equalsIgnoreCase(Params[Index])) {
        Index++;
        SerializedParam = ReadFile(Params[Index]);
        Index++;
      } else
        if ("-x".equalsIgnoreCase(Params[Index])) {
          Index++;
          SerializedParam = Params[Index];
          Index++;
        }
        else {
          throw new RuntimeException("Unknown flag." + Params[Index]);
        }

      OutputParams.add(DeserializeParam(SerializedParam));
    }

    return OutputParams.toArray();
  }


  public static Object DeserializeParam (String SerializedParam) {
    InputStream IStream = new ByteArrayInputStream(SerializedParam.getBytes()); 
    XMLDecoder decoder = new XMLDecoder(IStream);
    Object output = decoder.readObject();
    decoder.close(); 
    return output;
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="FileName"></param>
  /// <returns></returns>
  public static String ReadFile(String FileName) throws IOException {
    StringBuilder SBuilder = new StringBuilder();
    BufferedReader SReader = new BufferedReader(new FileReader(FileName));
    char[] CBuffer = new char[2048];
    int len;
    while((len=SReader.read(CBuffer)) > 0) {
      SBuilder.append(new String(CBuffer, 0, len));
    }
    return SBuilder.toString();
    
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="ParamValue"></param>
  /// <returns></returns>
  public static String SerializeParam(Object ParamValue) {
    ByteArrayOutputStream BStream = new ByteArrayOutputStream();
    XMLEncoder encoder = new XMLEncoder(BStream);
    encoder.writeObject(ParamValue);
    encoder.close();
    return BStream.toString();
  }


}
