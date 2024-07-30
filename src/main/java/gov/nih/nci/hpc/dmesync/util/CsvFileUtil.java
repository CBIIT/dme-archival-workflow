package gov.nih.nci.hpc.dmesync.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

public class CsvFileUtil {

	static final Logger logger = LoggerFactory.getLogger(CsvFileUtil.class);

	private CsvFileUtil() {
	}

	public static Map<String, Map<String, String>> parseBulkMetadataEntries(String metadataFile, String key)
			throws DmeSyncMappingException, IOException {

		if (StringUtils.isEmpty(metadataFile))
			return null;

		Map<String, Map<String, String>> metdataSheetMap = new HashMap<>();

        CSVParser parser = new CSVParserBuilder().withSeparator(detectDelimiter(metadataFile)).build(); 
 
		try (CSVReader reader = new CSVReaderBuilder
				(new FileReader(metadataFile)).withCSVParser(parser).build()) {
			// Read 1st row which is header row with attribute names
			String[] headers = reader.readNext();
			String[] row;
		    // Read all rows (skip 1st) and construct metadata map
			while ((row = reader.readNext()) != null) {
				String attrKey = null;
				int counter = 0;
				Map<String, String> rowMetadata = new HashMap<>();
				for (String attrName : headers) {
					String currentColumnvalue = row[counter];
					counter++;
					if (attrName.equalsIgnoreCase(key)) {
						attrKey = currentColumnvalue.trim();
						continue;
					}
					/*
					 * if (StringUtils.isNumeric(currentColumnvalue)) { double dv =
					 * Double.parseDouble(currentColumnvalue); if
					 * (HSSFDateUtil.isCellDateFormatted(currentCell)) { Date date =
					 * HSSFDateUtil.getJavaDate(dv); String df =
					 * currentCell.getCellStyle().getDataFormatString(); String strValue = new
					 * CellDateFormatter(df).format(date); rowMetadata.put(attrName.trim(),
					 * strValue); } else { DataFormatter dataFormatter = new DataFormatter(); String
					 * value = dataFormatter.formatCellValue(currentCell);
					 * rowMetadata.put(attrName.trim(), value); }
					 * 
					 * } else { if (currentColumnvalue != null && !currentColumnvalue.isEmpty())
					 * rowMetadata.put(attrName.trim(), currentColumnvalue.trim()); }
					 */

					if (currentColumnvalue != null && !currentColumnvalue.isEmpty())
						rowMetadata.put(attrName.trim(), currentColumnvalue.trim());

				}

				if (StringUtils.isNotBlank(attrKey))
					metdataSheetMap.put(attrKey, rowMetadata);

			}
		} catch (CsvValidationException | IOException e) {
			logger.error("Error reading metadata from csv  file {}", metadataFile);
			throw new DmeSyncMappingException("Error reading metadata from csv file", e);
		}
		return metdataSheetMap;
	}

	private static char detectDelimiter(String filename) throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line = br.readLine();
			if (line != null) {
				if (line.contains(",")) {
					return ',';
				} else if (line.contains("\t")) {
					return '\t';
				}
			}
		}
		throw new IOException("Unable to detect delimiter");
	}

}
