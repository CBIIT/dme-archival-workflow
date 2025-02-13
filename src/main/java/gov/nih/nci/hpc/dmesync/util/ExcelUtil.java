package gov.nih.nci.hpc.dmesync.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.format.CellDateFormatter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

public class ExcelUtil {

  static final Logger logger = LoggerFactory.getLogger(ExcelUtil.class);

  private ExcelUtil() {}

  public static String export(
      String runId, List<StatusInfo> statusInfo, List<MetadataInfo> metadataInfo, String path) {

    final String fileName = path + File.separatorChar + runId + ".xlsx";

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    SXSSFWorkbook workbook = new SXSSFWorkbook(100);

    try (FileOutputStream outputStream = new FileOutputStream(fileName); ) {

      int rowCount = 0;
      int colCount = 0;
      SXSSFSheet sheet = workbook.createSheet("RunResult");
      // create header row
      Row header = sheet.createRow(rowCount++);
      header.createCell(colCount++).setCellValue("RunId");
      header.createCell(colCount++).setCellValue("ObjectId");
      header.createCell(colCount++).setCellValue("OriginalFilePath");
      header.createCell(colCount++).setCellValue("FullDestinationPath");
      header.createCell(colCount++).setCellValue("Filesize");
      header.createCell(colCount++).setCellValue("HumanReadableFilesize");
      header.createCell(colCount++).setCellValue("Status");
      header.createCell(colCount++).setCellValue("TarContentsCount");
      header.createCell(colCount++).setCellValue("TarStartTimestamp");
      header.createCell(colCount++).setCellValue("TarEndTimestamp");
      header.createCell(colCount++).setCellValue("StartTimestamp");
      header.createCell(colCount++).setCellValue("EndTimestamp");
      header.createCell(colCount++).setCellValue("UploadStartTimestamp");
      header.createCell(colCount++).setCellValue("UploadEndTimestamp");
      header.createCell(colCount++).setCellValue("DataTransferRate(Bytes/Sec)");
      header.createCell(colCount++).setCellValue("Error");
      header.createCell(colCount++).setCellValue("RetryCount");
      header.createCell(colCount++).setCellValue("SourceFileName");

      Set<String> set = new HashSet<>(metadataInfo.size());
      metadataInfo.stream().filter(p -> set.add(p.getMetaDataKey())).collect(Collectors.toList());

      for (String key : set) {
        header.createCell(colCount++).setCellValue(key);
      }

      for (StatusInfo data : statusInfo) {
        colCount = 0;
        Row row = sheet.createRow(rowCount++);
        row.createCell(colCount++).setCellValue(data.getRunId());
        row.createCell(colCount++).setCellValue(data.getId());
        row.createCell(colCount++).setCellValue(data.getOriginalFilePath());
        row.createCell(colCount++).setCellValue(data.getFullDestinationPath());
        row.createCell(colCount++).setCellValue(data.getFilesize());
        row.createCell(colCount++).setCellValue(humanReadableByteCount(data.getFilesize().doubleValue(), true));
        row.createCell(colCount++).setCellValue(data.getStatus());
        if (data.getTarContentsCount() != null) {
			row.createCell(colCount++).setCellValue(data.getTarContentsCount());
		} else {
			row.createCell(colCount++).setCellValue("");
		}
        if (data.getTarStartTimestamp() != null && data.getTarEndTimestamp() != null) {
          row.createCell(colCount++).setCellValue(sdf.format(data.getTarStartTimestamp()));
          row.createCell(colCount++).setCellValue(sdf.format(data.getTarEndTimestamp()));
        } else {
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
        }
        row.createCell(colCount++).setCellValue(sdf.format(data.getStartTimestamp()));
        if ("COMPLETED".equalsIgnoreCase(data.getStatus())
            && data.getUploadStartTimestamp() != null) {
          if (data.getEndTimestamp() == null) data.setEndTimestamp(new Date());
          row.createCell(colCount++).setCellValue(sdf.format(data.getEndTimestamp()));
          row.createCell(colCount++).setCellValue(sdf.format(data.getUploadStartTimestamp()));
          row.createCell(colCount++).setCellValue(sdf.format(data.getUploadEndTimestamp()));
          //effective archival speed (bits/sec) per file (total amount of data moved) / (total time of archival process)
          Long ellapsedTimeInMilliSec =
              (data.getEndTimestamp().getTime() - data.getStartTimestamp().getTime());
          float rate = data.getFilesize() / (ellapsedTimeInMilliSec.floatValue() / 1000);
          //effective data transfer rates (only transfer)
          ellapsedTimeInMilliSec =
              (data.getUploadEndTimestamp().getTime() - data.getUploadStartTimestamp().getTime());
          rate = data.getFilesize() / (ellapsedTimeInMilliSec.floatValue() / 1000);
          row.createCell(colCount++).setCellValue(String.format("%.0f", rate));
        } else {
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
        }
        row.createCell(colCount++).setCellValue(data.getError());
        row.createCell(colCount++).setCellValue(data.getRetryCount());
        row.createCell(colCount++).setCellValue(data.getSourceFileName());
        for (String key : set) {
          boolean found = false;
          for (MetadataInfo metadata : metadataInfo) {
            if (metadata.getObjectId().equals(data.getId())
                && key.equals(metadata.getMetaDataKey())) {
              row.createCell(colCount++).setCellValue(metadata.getMetaDataValue());
              found = true;
              break;
            }
          }
          if (!found) {
            row.createCell(colCount++).setCellValue("");
          }
        }
      }

      workbook.write(outputStream);

    } catch (IOException e) {
      logger.error("Error writing to excel file {},  for runId {}", fileName, runId, e);
    } catch (Exception e) {
      logger.error("Error processing export to excel file {},  for runId {}", fileName, runId, e);
    } finally {
      workbook.dispose();
      try {
        workbook.close();
      } catch (IOException e) {
        logger.error("Error closing excel workbook {},  for runId {}", fileName, runId, e);
      }
    }
    return fileName;
  }

  public static Map<String, Map<String, String>> parseBulkMetadataEntries(
      String metadataFile, String key) throws DmeSyncMappingException {
    if (StringUtils.isEmpty(metadataFile)) return null;

    Map<String, Map<String, String>> metadataMap = null;
    Workbook workbook = null;

    try (FileInputStream fis = new FileInputStream(metadataFile)) {
      workbook = WorkbookFactory.create(fis);
      Sheet metadataSheet = workbook.getSheetAt(0);
      metadataMap = getMetadataMap(metadataSheet, key);
    } catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
      logger.error("Error reading metadata from excel file {}", metadataFile);
      throw new DmeSyncMappingException("Error reading metadata from excel file", e);
    } finally {
      if (workbook != null)
        try {
          workbook.close();
        } catch (IOException e) {
          logger.error("Error closing metadata from excel file {}", metadataFile);
        }
    }
    return metadataMap;
  }
  
  public static Map<String, Map<String, String>> parseMetadataTemplateEntries(
	      String metadataFile) throws DmeSyncMappingException {
    if (StringUtils.isEmpty(metadataFile)) return null;

    Map<String, Map<String, String>> metadataMap = null;
    Map<String, Map<String, String>> projectMap = null;
    Map<String, Map<String, String>> sampleMap = null;
    Workbook workbook = null;
    List<String> missingMetadata = null;
    StringBuffer sb = new StringBuffer();

    try (FileInputStream fis = new FileInputStream(metadataFile)) {
      workbook = WorkbookFactory.create(fis);
      Sheet metadataSheet = workbook.getSheet("Data Dictionary");
      metadataMap = getMandatoryMetadataMap(metadataSheet);
      Sheet projectSheet = workbook.getSheet("Project Template");
      projectMap = getProjectMetadataMap(projectSheet);
      Sheet sampleSheet = workbook.getSheet("Sample Template");
      sampleMap = getSampleMetadataMap(sampleSheet);
      missingMetadata = validateMandatoryMetadata(metadataMap.get("PI_Lab_Required"), projectMap.get("Project"));
      if(CollectionUtils.isNotEmpty(missingMetadata))
    	  sb.append("Missing mandatory metadata for PI_Lab: " + String.join(", ", missingMetadata) + "\n");
      missingMetadata = validateMandatoryMetadata(metadataMap.get("Project_Required"), projectMap.get("Project"));
      if(CollectionUtils.isNotEmpty(missingMetadata))
    	  sb.append("Missing mandatory metadata for Project: " + String.join(", ", missingMetadata) + "\n");
      for(Map.Entry<String, Map<String, String>> sampleEntry : sampleMap.entrySet()) {
    	  missingMetadata = validateMandatoryMetadata(metadataMap.get("Sample_Required"), sampleEntry.getValue());
    	  if(CollectionUtils.isNotEmpty(missingMetadata))
        	  sb.append("Missing mandatory metadata for " + sampleEntry.getKey() + ": " + String.join(", ", missingMetadata) + "\n");
      }
      if(StringUtils.isNotEmpty(sb.toString()))
    	  throw new DmeSyncMappingException(sb.toString());
      metadataMap.putAll(projectMap);
      metadataMap.putAll(sampleMap);
    } catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
      logger.error("Error reading metadata from excel file {}", metadataFile);
      throw new DmeSyncMappingException("Error reading metadata from excel file", e);
    } finally {
      if (workbook != null)
        try {
          workbook.close();
        } catch (IOException e) {
          logger.error("Error closing metadata from excel file {}", metadataFile);
        }
    }
    return metadataMap;
  }
  
  private static List<String> validateMandatoryMetadata(Map<String, String> mandatoryEntryMap, Map<String, String> metadataEntryMap) {
	  List<String> missingMetadata = new ArrayList<>();
	  for(Map.Entry<String, String> entry : mandatoryEntryMap.entrySet()) {
		  if (StringUtils.isBlank(metadataEntryMap.get(entry.getKey()))) {
			  missingMetadata.add(entry.getKey());
		  }
      }
	  return missingMetadata;
  }
  
  public static Map<String, Map<String, String>> parseBulkMetadataEntries(
      String metadataFile, String key1, String key2) throws DmeSyncMappingException {
    if (StringUtils.isEmpty(metadataFile)) return null;

    Map<String, Map<String, String>> metadataMap = null;
    Workbook workbook = null;

    try (FileInputStream fis = new FileInputStream(metadataFile)) {
      workbook = WorkbookFactory.create(fis);
      Sheet metadataSheet = workbook.getSheetAt(0);
      metadataMap = getMetadataMap(metadataSheet, key1, key2);
    } catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
      logger.error("Error reading metadata from excel file {}", metadataFile);
      throw new DmeSyncMappingException("Error reading metadata from excel file", e);
    } finally {
      if (workbook != null)
        try {
          workbook.close();
        } catch (IOException e) {
          logger.error("Error closing metadata from excel file {}", metadataFile);
        }
    }
    return metadataMap;
  }
  
	public static void convertTextToExcel(File textFile, File excelFile, String delimiter) throws IOException {

		// Sets up the Workbook and gets the 1st (0) sheet.
		HSSFWorkbook workbook = new HSSFWorkbook();
		HSSFSheet sheet = workbook.createSheet("Sheet1");

		int rowNo = 0;
		int columnNo = 0;

		Scanner scanner = new Scanner(textFile);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			// Create a new row
			HSSFRow tempRow = sheet.createRow(rowNo);

			Scanner lineScanner = new Scanner(line);
			lineScanner.useDelimiter(delimiter);
			// While there is more text to get it will loop.
			while (lineScanner.hasNext()) {
				// Creates the cell in that row.
				Cell tempCell = tempRow.createCell(columnNo);
				String output = lineScanner.next();
				// Write the output to that cell.
				tempCell.setCellValue(new HSSFRichTextString(output));
				columnNo++;
			}
			lineScanner.close();
			// Resets the column count for the new row.
			columnNo = 0;
			rowNo++;
		}
		scanner.close();

		// Writes the file and closes everything.
		FileOutputStream out = new FileOutputStream(excelFile);
		workbook.write(out);
		workbook.close();
		out.close();
	}

  private static List<String> getHeader(Sheet metadataSheet, String key, Integer rowNum)
      throws DmeSyncMappingException {
    List<String> header = new ArrayList<>();
    Integer rowNumber = rowNum == null ? metadataSheet.getFirstRowNum() : rowNum;
    Row firstRow = metadataSheet.getRow(rowNumber);
    Iterator<Cell> cellIterator = firstRow.iterator();
    while (cellIterator.hasNext()) {
      Cell currentCell = cellIterator.next();
      String cellValue = currentCell.getStringCellValue();
      if (cellValue == null || cellValue.isEmpty())
        throw new DmeSyncMappingException(
            "Empty header column value in column " + currentCell.getColumnIndex());
      header.add(cellValue);
    }
    if (!header.contains(key))
      throw new DmeSyncMappingException("Key: " + key + " header column is missing");
    return header;
  }
  
  private static List<String> getHeader(Sheet metadataSheet, String key1, String key2, Integer rowNum)
      throws DmeSyncMappingException {
    List<String> header = new ArrayList<>();
    Row firstRow = metadataSheet.getRow(metadataSheet.getFirstRowNum());
    Iterator<Cell> cellIterator = firstRow.iterator();
    while (cellIterator.hasNext()) {
      Cell currentCell = cellIterator.next();
      String cellValue = currentCell.getStringCellValue();
      if (cellValue == null || cellValue.isEmpty())
        throw new DmeSyncMappingException(
            "Empty header column value in column " + currentCell.getColumnIndex());
      header.add(cellValue.trim());
    }
    if (!header.contains(key1) || !header.contains(key2))
      throw new DmeSyncMappingException("Key: " + key1 + " or Key: " + key2 + " header column is missing");
    return header;
  }

  private static Map<String, Map<String, String>> getMetadataMap(Sheet metadataSheet, String key)
      throws DmeSyncMappingException {
    Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();
    Iterator<Row> iterator = metadataSheet.iterator();

    // Read 1st row which is header row with attribute names
    List<String> attrNames = getHeader(metadataSheet, key, null);
    // Read all rows (skip 1st) and construct metadata map
    // Skip cells exceeding header size
    while (iterator.hasNext()) {
      String attrKey = null;
      Row currentRow = iterator.next();
      if (currentRow.getRowNum() == 0) continue;
      // Skip header row
      int counter = 0;
      Map<String, String> rowMetadata = new HashMap<>();

      for (String attrName : attrNames) {
        Cell currentCell = currentRow.getCell(counter);
        counter++;
        if (currentCell == null) continue;
        if (attrName.equalsIgnoreCase(key)) {
          attrKey = currentCell.getStringCellValue().trim();
          continue;
        }
        if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
          double dv = currentCell.getNumericCellValue();
          if (HSSFDateUtil.isCellDateFormatted(currentCell)) {
            Date date = HSSFDateUtil.getJavaDate(dv);
            String df = currentCell.getCellStyle().getDataFormatString();
            String strValue = new CellDateFormatter(df).format(date);
            rowMetadata.put(attrName.trim(), strValue);
          } else {
        	DataFormatter dataFormatter = new DataFormatter();
        	String value = dataFormatter.formatCellValue(currentCell);
            rowMetadata.put(attrName.trim(), value);
          }

        } else if (currentCell.getCellTypeEnum().equals(CellType.BOOLEAN)) {
        	Boolean value = currentCell.getBooleanCellValue();
        	rowMetadata.put(attrName.trim(), value.toString());
        } else {
          if (currentCell.getStringCellValue() != null
              && !currentCell.getStringCellValue().isEmpty())
            rowMetadata.put(attrName.trim(), currentCell.getStringCellValue().trim());
        }
      }
      if(StringUtils.isNotBlank(attrKey))
    	  metdataSheetMap.put(attrKey, rowMetadata);
    }

    return metdataSheetMap;
  }
  
  private static Map<String, Map<String, String>> getSampleMetadataMap(Sheet metadataSheet)
	      throws DmeSyncMappingException {
    Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();
    Iterator<Row> iterator = metadataSheet.iterator();

    List<String> attrNames = null;
    while (iterator.hasNext()) {
      String attrKey = null;
      Row currentRow = iterator.next();
      if (currentRow.getRowNum() == 0 || currentRow.getRowNum() == 1) continue;
      Cell currentCell = currentRow.getCell(0);
      if (currentCell.getStringCellValue().equalsIgnoreCase("Sample ID")) {
    	  attrNames = getHeader(metadataSheet, "Sample ID", currentRow.getRowNum());
    	  continue;
      } else if (currentCell.getStringCellValue().startsWith("Optional Fields")) {
    	  continue;
      }
      int counter = 0;
      Map<String, String> rowMetadata = new HashMap<>();

      for (String attrName : attrNames) {
        currentCell = currentRow.getCell(counter);
        counter++;
        if (currentCell == null) continue;
        if (attrName.equalsIgnoreCase("Sample ID") && StringUtils.isNotBlank(currentCell.getStringCellValue())) {
          attrKey = currentCell.getStringCellValue();
        }
        if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
          double dv = currentCell.getNumericCellValue();
          if (HSSFDateUtil.isCellDateFormatted(currentCell)) {
            Date date = HSSFDateUtil.getJavaDate(dv);
            String df = currentCell.getCellStyle().getDataFormatString();
            String strValue = new CellDateFormatter(df).format(date);
            rowMetadata.put(attrName.trim(), strValue);
          } else {
        	DataFormatter dataFormatter = new DataFormatter();
        	String value = dataFormatter.formatCellValue(currentCell);
            rowMetadata.put(attrName.trim(), value);
          }

        } else if (currentCell.getCellTypeEnum().equals(CellType.BOOLEAN)) {
        	Boolean value = currentCell.getBooleanCellValue();
        	rowMetadata.put(attrName.trim(), value.toString());
        } else {
          if (currentCell.getStringCellValue() != null
              && !currentCell.getStringCellValue().isEmpty())
            rowMetadata.put(attrName.trim(), currentCell.getStringCellValue());
        }
      }
      if(StringUtils.isNotBlank(attrKey))
    	  if(metdataSheetMap.get(attrKey) == null) {
    		  metdataSheetMap.put(attrKey, rowMetadata);
    	  } else  {
    		  metdataSheetMap.get(attrKey).putAll(rowMetadata);
    	  }
    }

    return metdataSheetMap;
  }
  
  private static Map<String, Map<String, String>> getProjectMetadataMap(Sheet metadataSheet) {
	Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();
    Iterator<Row> iterator = metadataSheet.iterator();

    // Read all rows and construct Project metadata map
    while (iterator.hasNext()) {
      String attrKey = "Project";
      Row currentRow = iterator.next();
      if (currentRow.getRowNum() == 0) continue;
      // Skip title row
      Map<String, String> rowMetadata = new HashMap<>();

      Cell currentCell = currentRow.getCell(0);
      if (currentCell == null) 
    	  continue;
      String attrName = currentCell.getStringCellValue();
      currentCell = currentRow.getCell(1);
      if (currentCell == null) 
    	  continue;
      if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
    	  double dv = currentCell.getNumericCellValue();
          if (HSSFDateUtil.isCellDateFormatted(currentCell)) {
            Date date = HSSFDateUtil.getJavaDate(dv);
            String df = currentCell.getCellStyle().getDataFormatString();
            String strValue = new CellDateFormatter(df).format(date);
            rowMetadata.put(attrName.trim(), strValue);
          } else {
        	DataFormatter dataFormatter = new DataFormatter();
        	String value = dataFormatter.formatCellValue(currentCell);
            rowMetadata.put(attrName.trim(), value);
          }
      } else {
      if (currentCell.getStringCellValue() != null
          && !currentCell.getStringCellValue().isEmpty())
        rowMetadata.put(attrName.trim(), currentCell.getStringCellValue());
      }
      if(metdataSheetMap.get(attrKey) == null)
    	  metdataSheetMap.put(attrKey, rowMetadata);
      else
    	  metdataSheetMap.get(attrKey).putAll(rowMetadata);
    }

    return metdataSheetMap;
  }
  
  private static Map<String, Map<String, String>> getMandatoryMetadataMap(Sheet metadataSheet) {
    Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();
    Iterator<Row> iterator = metadataSheet.iterator();

    // Read all rows and construct Project metadata map
    String attrKey = null;
    while (iterator.hasNext()) {
      Row currentRow = iterator.next();
      Map<String, String> rowMetadata = new HashMap<>();

      Cell currentCell = currentRow.getCell(0);
      if (currentCell == null) 
    	  continue;
      if (currentCell.getStringCellValue().equalsIgnoreCase("PI_Lab Collection")) {
    	  attrKey = "PI_Lab";
    	  continue;
      } else if (currentCell.getStringCellValue().equalsIgnoreCase("Project Collection")) {
    	  attrKey = "Project";
    	  continue;
      } else if (currentCell.getStringCellValue().equalsIgnoreCase("Sample Collection")) {
    	  attrKey = "Sample";
    	  continue;
      }
      
      String required = currentCell.getStringCellValue().trim();
      String key = attrKey + "_" + required;
      currentCell = currentRow.getCell(1);
      if (currentCell == null) 
    	  continue;
      String attrName = currentCell.getStringCellValue();
      currentCell = currentRow.getCell(2);
      String attrValue = currentCell.getStringCellValue();
      rowMetadata.put(attrName.trim(), attrValue.trim());
      if(metdataSheetMap.get(key) == null)
    	  metdataSheetMap.put(key, rowMetadata);
      else
    	  metdataSheetMap.get(key).putAll(rowMetadata);
    }

    return metdataSheetMap;
  }
  
  private static Map<String, Map<String, String>> getMetadataMap(Sheet metadataSheet, String key1, String key2)
      throws DmeSyncMappingException {
    Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();
    Iterator<Row> iterator = metadataSheet.iterator();

    // Read 1st row which is header row with attribute names
    List<String> attrNames = getHeader(metadataSheet, key1, key2, null);
    // Read all rows (skip 1st) and construct metadata map
    // Skip cells exceeding header size
    while (iterator.hasNext()) {
      String attrKey1 = null;
      String attrKey2 = null;
      Row currentRow = iterator.next();
      if (currentRow.getRowNum() == 0) continue;
      // Skip header row
      int counter = 0;
      Map<String, String> rowMetadata = new HashMap<>();

      for (String attrName : attrNames) {
        Cell currentCell = currentRow.getCell(counter);
        counter++;
        if (currentCell == null) continue;
        if (attrName.equalsIgnoreCase(key1)) {
          if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
            double dv = currentCell.getNumericCellValue();
            attrKey1 = String.format ("%.0f", dv);
          } else
            attrKey1 = currentCell.getStringCellValue();
          rowMetadata.put(attrName.trim(), attrKey1);
          continue;
        } else if (attrName.equalsIgnoreCase(key2)) {
          if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
            double dv = currentCell.getNumericCellValue();
            attrKey2 = String.format ("%.0f", dv);
          } else
            attrKey2 = currentCell.getStringCellValue();
          rowMetadata.put(attrName.trim(), attrKey2);
          continue;
        }
        if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
          double dv = currentCell.getNumericCellValue();
          if (HSSFDateUtil.isCellDateFormatted(currentCell)) {
            Date date = HSSFDateUtil.getJavaDate(dv);
            String df = currentCell.getCellStyle().getDataFormatString();
            String strValue = new CellDateFormatter(df).format(date);
            rowMetadata.put(attrName.trim(), strValue);
          } else {
            rowMetadata.put(attrName.trim(), String.format ("%.0f", dv));
          }

        } else if (currentCell.getCellTypeEnum().equals(CellType.BOOLEAN)) {
        	Boolean dv = currentCell.getBooleanCellValue();
        	rowMetadata.put(attrName.trim(), dv.toString());
        } else {
          if (currentCell.getStringCellValue() != null
              && !currentCell.getStringCellValue().isEmpty())
            rowMetadata.put(attrName.trim(), currentCell.getStringCellValue());
        }
      }

      metdataSheetMap.put(attrKey1 + "_" + attrKey2, rowMetadata);
    }

    return metdataSheetMap;
  }
  
  private static final String[] SI_UNITS = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
  private static final String[] BINARY_UNITS = { "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };

  public static String humanReadableByteCount(final double bytes, final boolean useSIUnits) {
	final String[] units = useSIUnits ? SI_UNITS : BINARY_UNITS;
	final int base = useSIUnits ? 1000 : 1024;

	// When using the smallest unit no decimal point is needed, because it's
	// the exact number.
	if (bytes < base) {
		return bytes + " " + units[0];
	}

	final int exponent = (int) (Math.log(bytes) / Math.log(base));
	final String unit = units[exponent];
	return String.format("%.1f %s", bytes / Math.pow(base, exponent), unit);
  }
}
