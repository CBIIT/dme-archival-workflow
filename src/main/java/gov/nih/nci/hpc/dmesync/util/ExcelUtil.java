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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.format.CellDateFormatter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
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
      //header.createCell(colCount++).setCellValue("OrginalFileName");
      header.createCell(colCount++).setCellValue("OriginalFilePath");
      //header.createCell(colCount++).setCellValue("SourceFileName");
      //header.createCell(colCount++).setCellValue("SourceFilePath");
      header.createCell(colCount++).setCellValue("FullDestinationPath");
      header.createCell(colCount++).setCellValue("Filesize");
      header.createCell(colCount++).setCellValue("Status");
      header.createCell(colCount++).setCellValue("TarStartTimestamp");
      header.createCell(colCount++).setCellValue("TarEndTimestamp");
      header.createCell(colCount++).setCellValue("StartTimestamp");
      header.createCell(colCount++).setCellValue("EndTimestamp");
      header.createCell(colCount++).setCellValue("UploadStartTimestamp");
      header.createCell(colCount++).setCellValue("UploadEndTimestamp");
      //header.createCell(colCount++).setCellValue("ArchiveSpeed(Bytes/Sec)");
      header.createCell(colCount++).setCellValue("DataTransferRate(Bytes/Sec)");
      header.createCell(colCount++).setCellValue("Error");
      header.createCell(colCount++).setCellValue("RetryCount");

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
        //row.createCell(colCount++).setCellValue(data.getOrginalFileName());
        row.createCell(colCount++).setCellValue(data.getOriginalFilePath());
        //row.createCell(colCount++).setCellValue(data.getSourceFileName());
        //row.createCell(colCount++).setCellValue(data.getSourceFilePath());
        row.createCell(colCount++).setCellValue(data.getFullDestinationPath());
        row.createCell(colCount++).setCellValue(data.getFilesize());
        row.createCell(colCount++).setCellValue(data.getStatus());
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
          //row.createCell(colCount++).setCellValue(String.format ("%.0f", rate));
          //effective data transfer rates (only transfer)
          ellapsedTimeInMilliSec =
              (data.getUploadEndTimestamp().getTime() - data.getUploadStartTimestamp().getTime());
          rate = data.getFilesize() / (ellapsedTimeInMilliSec.floatValue() / 1000);
          row.createCell(colCount++).setCellValue(String.format("%.0f", rate));
        } else {
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
          //row.createCell(colCount++).setCellValue("");
          row.createCell(colCount++).setCellValue("");
        }
        row.createCell(colCount++).setCellValue(data.getError());
        row.createCell(colCount++).setCellValue(data.getRetryCount());
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

  public static Map<String, Map<String, String>> parseBulkMatadataEntries(
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

  private static List<String> getHeader(Sheet metadataSheet, String key)
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
      header.add(cellValue);
    }
    if (!header.contains(key))
      throw new DmeSyncMappingException("Key: " + key + " header column is missing");
    return header;
  }

  private static Map<String, Map<String, String>> getMetadataMap(Sheet metadataSheet, String key)
      throws DmeSyncMappingException {
    Map<String, Map<String, String>> metdataSheetMap = new HashMap<String, Map<String, String>>();
    Iterator<Row> iterator = metadataSheet.iterator();

    // Read 1st row which is header row with attribute names
    List<String> attrNames = getHeader(metadataSheet, key);
    // Read all rows (skip 1st) and construct metadata map
    // Skip cells exceeding header size
    while (iterator.hasNext()) {
      String attrKey = null;
      Row currentRow = iterator.next();
      if (currentRow.getRowNum() == 0) continue;
      // Skip header row
      int counter = 0;
      Map<String, String> rowMetadata = new HashMap<String, String>();

      for (String attrName : attrNames) {
        Cell currentCell = currentRow.getCell(counter);
        counter++;
        if (currentCell == null) continue;
        if (attrName.equalsIgnoreCase(key)) {
          attrKey = currentCell.getStringCellValue();
          continue;
        }
        if (currentCell.getCellTypeEnum().equals(CellType.NUMERIC)) {
          double dv = currentCell.getNumericCellValue();
          if (HSSFDateUtil.isCellDateFormatted(currentCell)) {
            Date date = HSSFDateUtil.getJavaDate(dv);
            String df = currentCell.getCellStyle().getDataFormatString();
            String strValue = new CellDateFormatter(df).format(date);
            rowMetadata.put(attrName, strValue);
          } else {
            rowMetadata.put(attrName, (new Double(dv).toString()));
          }

        } else {
          if (currentCell.getStringCellValue() != null
              && !currentCell.getStringCellValue().isEmpty())
            rowMetadata.put(attrName, currentCell.getStringCellValue());
        }
      }

      metdataSheetMap.put(attrKey, rowMetadata);
    }

    return metdataSheetMap;
  }
}
