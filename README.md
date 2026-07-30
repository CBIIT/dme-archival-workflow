# DME Auto-Archival Workflow

**A customizable workflow to auto-archive large dataset to DME object storage.**  This standalone application can 
be installed and run on any windows/linux client machine where the data to be archived is accessible. 
The collections and data sets to be archived are uploaded to NCI DME Data Management Environment 
at a scheduled interval or for a one time archival. Fault tolerance and multi-threading capabilities are built-in to 
achieve reliability and high throughput. It can be customized to derive the DME archival path and 
extract metadata for collection and data object to be supplied to DME based on the folder structure 
and file naming convention and/or user provided mapping data.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes. See deployment for notes on how to deploy the project on a client machine.

### Prerequisites

```
Java 8, Maven, Git
```

### Installing

This will tell you how to get a development env running

##### Get Your App

To check out the project from git, do:

```
$ git clone https://github.com/CBIIT/HPC_DME_APIs
$ git clone https://github.com/CBIIT/dme-archival-workflow
```

##### Build Your App
Download ojdbc6 from group com.oracle.database.jdbc (version 11.2.0.4) and install into your local maven repository.

Navigate to the project and build:

```
$ cd HPC_DME_APIs/src
$ mvn -pl "hpc-server/hpc-domain-types,hpc-server/hpc-common,hpc-server/hpc-dto" clean install
$ cd dme-archival-workflow
$ mvn clean install -DskipTests
```

##### Start Your App
Please run the script to generate the token.
```
$ sh dme-sync-generate-token.sh
```
You will be prompted for your username and password and which environment you will be running the workflow against.
* `env=[dev|uat|prod|prod2|prod3|prod4|prod_bp]`
Prod config depends on which DME API server to connect to.

The application uses an oracle database:

```
To access the database, use tool such as Datagrip

In src/main/resources/application.properties, update the following with the correct password
spring.datasource.password=<updated here>
```

Run the application locally:

Check if the dme-sync-[verison].jar entry in dme-sync.sh matches with the version built. If not, update it.
```
$ sh dme-sync.sh
```

##### Test it

To verify that the application is running, open the browser and access the default app page (Work in progress):

```
http://localhost:8888/home
```


## Deployment

This instruction will cover how to deploy this on a client machine

#### Prerequisites

Before installing it on the remote machine, `application.propterties` shall be configured properly. 
See [Customizing the Workflow](#customizing-the-workflow) section.

#### Running on the remote machine
Upload the jar, bash script, configuration file (application.properties) to the client machine:
```
$ scp target/dme-sync-<version>.jar user@remotemachine:~/
$ scp dme-sync-generate-token.sh user@remotemachine:~/
$ scp dme-sync.sh user@remotemachine:~/
$ scp application.properties user@remotemachine:~/
```

Log into the remote machine and start the application
```
$ ssh user:pass@remotemachine
$ sh dme-sync-generate-token.sh
$ sh dme-sync.sh
```

On your local machine, perform the following to tunnel port 8888:
```
$ ssh -L 8887:localhost:8888 user@remotemachine
```

Now the default app page can be accessed from your local browser:
```
http://localhost:8887/home
```

## Customizing the Workflow

### Configuration Parameters

The following properties can be configured in the tables:

#### Table: DOC_CONFIG (Stores the DOC workflow configuration)
* `DOC_NAME` - DOC name.
* `SERVER_ID` - Server identifier.
* `WORKFLOW_ID` - Unique workflow identifier.
* `DME_SERVER_ID` - DME server identifier.
* `DME_SERVER_URL` - DME server URL.
* `THREADS` - Number of processing threads.
* `ENABLED` - Indicates whether this DOC configuration is enabled. 1 = enabled, 0 = disabled.
* `CRON_EXPRESSION` - Cron expression used to schedule the workflow.

---
#### Table: DOC_SOURCE_CONFIG (Stores source, work, and destination directory configuration for a DOC workflow)
* `SOURCE_BASE_DIR` - Base source directory where the app will scan for files/folders to upload.
* `WORK_BASE_DIR` - Base working directory used during processing of tar/untar and compression.
* `DESTINATION_BASE_DIR` - Base collection path in DME.

---
#### Table: DOC_SOURCE_RULE (Stores source selection and scanning rules for a DOC workflow)
* `SOURCE_BASE_DIR_FOLDERS` - Source base directory folders to include or process.
* `INCLUDE_PATTERN` - File or folder include pattern. Comma-separated multiple patterns can be specified.
* `EXCLUDE_PATTERN` - File or folder exclude pattern. If both include and exclude pattern is applicable for a file/folder, the file/folder will be excluded as the exclusion takes precedence.
* `METADATA_FILE` - Metadata file name or path. If specified, application will load the custom metadata excel file supplied by the user.
* `PI_METADATA_FILE` - PI metadata file name or path. If specified, application will load the custom data owner PI metadata excel file supplied by the user.
* `NOSCAN_RERUN` - Indicates whether rerun should occur without scanning. 1 = yes, 0 = no. If true, instead of scanning for files under the base dir, it will reprocess files from the database.
* `FILE_EXIST_UNDER_BASEDIR` - Check if the marker file specified in `TAR_FILE_EXIST_EXT` is directly under the base directory. 1 = yes, 0 = no.
* `FILE_EXIST_UNDER_BASEDIR_DEPTH` - If specified, it will check for the file under the specified depth from the basedir.
* `LAST_MODIFIED_DAYS` - Number of days used for last-modified filtering. If specified, if modified date of the file/folder is within the number of days specified, it will not be archived.
* `LAST_MODIFIED_UNDER_BASE_DIR` - Indicates whether last-modified filtering under the base directory is enabled. 1 = yes, 0 = no. If specified, checks modified date of the folder in the base directory instead of folder/file.
* `LAST_MODIFIED_UNDER_BASE_DIR_DEPTH` - Directory depth for last-modified filtering under the base directory. If specified, it will check for the last modified folder under the specified depth from the basedir.
* `SELECTIVE_SCAN` - Indicates whether selective scan is enabled. 1 = yes, 0 = no.
* `RETRY_PRIOR_RUN_FAILURES` - Indicates whether prior run failures should be retried. 1 = yes, 0 = no. It is mainly used when we are not scanning the entire source path, but only specific months.
* `AWS` - Indicates whether AWS-based source handling is enabled. 1 = yes, 0 = no.

---
#### Table: DOC_PREPROCESSING_CONFIG (Stores preprocessing configuration for a DOC workflow)
* `TAR` - Indicates whether tar processing is enabled. 1 = yes, 0 = no. If true, it will tar the collection specified by `DEPTH` from `SOURCE_BASE_DIR`.
* `DEPTH` - Directory depth used during tar or untar. It is used to determine the folder to tar/untar. If -1 is specified with tar option, it will tar the leaf folder only.
* `FILE_TAR` - Indicates whether file-level tar processing is enabled. 1 = yes, 0 = no.
* `UNTAR` - Indicates whether untar processing is enabled. 1 = yes, 0 = no. If true, it will untar the collection specified by `DEPTH` from `SOURCE_BASE_DIR`.
* `COMPRESS_TAR` - Indicates whether tar compression is enabled. 1 = yes, 0 = no. If true, the data object will be compressed prior to upload.

```SOURCE_BASE_DIR = /home/user/instrument
    ├── instrument                        # Depth 0
    │   ├── pi_a                          # Depth 1
    │       ├── project1                  # Depth 2
    │           ├── sample1               # Depth 3
    │           ├── sample2               # Depth 3
    │       ├── project2                  # Depth 2
    │   ├── pi_b                          # Depth 1
    │       ├── project1                  # Depth 2
    │           ├── sample1               # Depth 3
    │           ├── sample2               # Depth 3
    │       ├── project2                  # Depth 2
```

---
#### Table: DOC_PREPROCESSING_RULE (Stores detailed preprocessing rules for a DOC workflow)
* `EXTRACT_METADATA` - Indicates whether metadata extraction is enabled. 1 = yes, 0 = no.
* `EXTRACT_METADATA_EXT` - File extension used for metadata extraction.
* `TAR_EXCLUDE_FOLDER` - List of comma separated folders to exclude from tar processing. If specified, folders that match the folder names in a comma separated list will be excluded from the tar ball.
* `TAR_CONTENTS_FILE` - Indicates whether a tar contents file should be generated which includes files in each tar and archive. 1 = yes, 0 = no.
* `TAR_EXCLUDED_CONTENTS_FILE` - Indicates whether the system will enable creation of a tar contents file to list the files that are included in a tar archive which are broken symlinks. 1 = yes, 0 = no.
* `TAR_FILE_EXIST` - If specified, it will check whether a file with the specified file name exists before tar operation is performed.
* `TAR_FILE_EXIST_EXT` - If specified, it will check whether a file with the specified file extension exists before tar operation is performed.
* `TAR_FILENAME_EXCEL_EXIST` - Indicates whether to retrieve the tar name from the excel spreadsheet. 1 = yes, 0 = no.
* `TAR_IGNORE_BROKEN_LINK` - Indicates whether broken links should be ignored during tar processing. 1 = yes, 0 = no. If set to false, the tar will not be created and the error from broken links will be recorded in the email report. If it is set to true, these errors will be ignored and the tar will be created. Default is false.
* `TAR_SKIP_NON_LEAF_FOLDER` - Indicates whether non-leaf folders should be skipped during tar processing. 1 = yes, 0 = no.
* `TAR_INCLUDE_PATTERN` - Include pattern for tar processing.
* `PROCESS_MULTIPLE_TARS` - Indicates whether to create batch tars for the folder with the count specified by MULTIPLE_TARS_FILES_COUNT. 1 = yes, 0 = no.
* `MULTIPLE_TARS_FILES_COUNT` - The number of files to be batched in a single tar when the multiple tar mode is enabled.
* `MULTIPLE_TARS_DIR_FOLDERS` - Directory folders used for multiple tar processing.
* `MULTIPLE_TARS_DIR_FOLDERS_PREFIX` - Folder prefix used for multiple tar directory selection.
* `MULTIPLE_TARS_EXCLUDE_FOLDERS_PREFIX` - Folder prefix used to exclude folders from multiple tar processing.
* `MULTIPLE_TARS_FILES_VALIDATION` - Indicates whether multiple tar file validation is enabled. 1 = yes, 0 = no.
* `MULTIPLE_TARS_BATCH_FOLDERS` - Indicates whether grouped/batched folder mode for multiple tar processing is enabled. 1 = yes, 0 = no. If true, the workflow will create one tar per folder-group derived from the folder naming convention, instead of creating tars based on count.
* `MULTIPLE_TARS_BATCH_FOLDER_DELIMITER` - Delimiter used for multiple tar batch folder parsing. Delimiter used to split each immediate child folder name (under the dataset directory) into segments to derive the group key.
* `MULTIPLE_TARS_BATCH_FOLDER_DELIMITER_LEVEL` - Delimiter level used for multiple tar batch folder parsing. Number of leading segments (after splitting by dmesync.multiple.tars.batch.folder.delimiter) to join back together to form the group key.
    * Example:
      * folder `1_11_3`, delimiter `_`, level `2` → group key `1_11` → tar name `1_11.tar`
      * folder `a-b-c-d`, delimiter `-`, level `3` → group key `a-b-c` → tar name `a-b-c.tar`
    * Notes: 
      * When batching is enabled, all folders being grouped must match the naming convention (i.e., they must have at least `level` segments). If not, the workflow will fail verification.

---
#### Table: DOC_UPLOAD_CONFIG (Stores upload behavior and post-processing configuration for a DOC workflow)
* `VERIFY_PREV_UPLOAD` - Indicates whether previous uploads should be verified. 1 = yes, 0 = no. If true, it will check the local db if it has previously been uploaded, and skip the file.
* `DRY_RUN` - Indicates whether upload should run in dry-run mode. 1 = yes, 0 = no. If true, only records the files to be processed in the local DB without running the workflow.
* `CHECKSUM` - Indicates whether checksum validation is enabled. 1 = yes, 0 = no.
* `CLEANUP_WORKDIR` - Indicates whether the work directory should be cleaned up after processing. 1 = yes, 0 = no.
* `CHECK_END_WORKFLOW` - Indicates whether end-of-workflow checks are enabled. 1 = yes, 0 = no. If true, the system will check for endWorkflow flag to end task processing if no further processing is necessary.
* `UPLOAD_MODIFIED_FILES` - Indicates whether modified files should be uploaded. 1 = yes, 0 = no. If true, the system will compare the new file checksum and file length with archived file and if doesn't match will append _ver_fileLastModifiedDate to the filename and archive to DME.
* `REPLACE_MODIFIED_FILES` - Indicates whether modified files should replace existing files. 1 = yes, 0 = no. If true, the system will compare the modified date against the last uploaded and reupload if modified.
* `METADATA_UPDATE_ONLY` - Indicates whether only metadata should be updated. 1 = yes, 0 = no.
* `MOVE_PROCESSED_FILES` - Indicates whether processed files should be moved after upload. 1 = yes, 0 = no.
* `FILESYSTEM_UPLOAD` - Indicates whether filesystem upload mode is enabled. 1 = yes, 0 = no.
* `SOFTLINK` - Indicates whether softlink creation is enabled. 1 = yes, 0 = no.
* `COLLECTION_SOFTLINK` - Indicates whether collection-level softlink creation is enabled. 1 = yes, 0 = no.
* `SOFTLINK_FILE` - Softlink file path or file name.

---
#### Table: DOC_NOTIFICATION_CONFIG (Stores notification settings for a DOC workflow)
* `RECIPIENTS` - Notification recipient list. Once a run completes, the run result will be emailed to this address.
* `SEND_USER` - Indicates whether notifications are enabled for HiTIF configured users. 1 = yes, 0 = no.
* `ENABLED` - Indicates whether notifications are enabled. 1 = enabled, 0 = disabled.

---
#### Optionally, override system defaults for concurrent file processing with the following parameters:

* Number of threads to process the files concurrently
    ```
    spring.jms.listener.concurrency=<min number of threads>
    spring.jms.listener.max-concurrency=<max number of threads>
    ```
* Number of threads to upload multi-part upload file parts concurrently
    ```
    dmesync.multipart.threadpoolsize=<number of threads>
    ```
### Static metadata entries and collection name mapping

The DOC specific metadata entries and collection name mapping can be inserted into the mapping tables:
Please see sample `data.sql` provided to supply custom metadata mapping and collection name mapping.
Refer to [Required mapping for customized DOC](#required-mapping-for-customized-doc) section for further 
configuration on existing DOCs.

#### collection_name_mapping table
```
| id | collection_type | map_key | map_value |
|----|-----------------|---------|-----------|
|  1 |              PI |    pi_a | PI_A_NAME |
|  2 |              PI |    pi_b | PI_B_NAME |
```
#### metadata_mapping table
```
| id | collection_type | collection_name | map_key |   map_value   | 
|----|-----------------|-----------------|---------|---------------| 
|  1 |              PI |       PI_A_NAME |full_name|   Jane Doe    | 
|  2 |              PI |       PI_A_NAME |   email | email@address |
```
### Adding User permissions and bookmarks

This feature is only available if you are running the workflow as a **GROUP_ADMIN** role.
Once the collection/data objects are archived, if user permission (**READ, WRITE, OWN**) or bookmark needs to be added, 
it could be specified in the following table. The entries can be loaded by including it in the `data.sql` file 
for the metadata entries.

####  permission_bookmark_info table
```
| id |    path    | user_id | permission | create_bookmark |     created    |    error     |
|----|------------|---------|------------|-----------------|----------------|--------------|
|  1 | /DME/path1 |  usera  |    READ    |          Y      | "Y" if created | Error if any |
|  2 | /DME/path2 |  userb  |    WRITE   |          N      | "Y" if created | Error if any |
```
For the example above in the table, for id 1, READ permission will be given to usera for /DME/path1, and a bookmark 
called "path1" will be created for usera for /DME/path1. For id 2, WRITE permission will be given to userb for 
/DME/path2 and no bookmark will be created.

## Exporting Archival Result to a CSV or Excel file

From the Web interface, you can export any run into a Excel (xlsx) file.
The file will be generated in your application log directory and emailed to the address specified in application.properties file.
If the runId is not specified, it will export the latest runId.
```
http://localhost:8888/export
http://localhost:8888/export/{runId}
```

## Built With

* [Spring Boot](https://spring.io/projects/spring-boot) - Framework used
* [Maven](https://maven.apache.org/) - Dependency Management

## License

For license details for this project, see [LICENSE.txt](LICENSE.txt) file

## Required mapping for customized DOC

### HiTIF Workflow
The following information is required in the **collection_name_mapping**.

* **PI** collection_type, key and value (key will be the user folder, value is the PI collection in DME to map to)
* **User** collection_type, key and value (key will be the user folder, value is the User collection in DME to map to)

The following information is required in the **metadata_mapping**.
* For **PI** collection_type:
  * collection_type: `PI`
  * pi_name
  * pi_email
  * institute
  * lab
  * branch

* For **User** collection_type:
  * collection_type: `User`
  * name
  * email
  * branch

### CMM Workflow
The following information is required in the **metadata_mapping**.
* For **PI** collection_type:
  * collection_type: `PI_Lab`
  * pi_name
  * affiliation
  * pi_id

* For **Project** collection_type:
  * collection_type: `Project`
  * project_name
  * project_number
  * start_date
  * method
  * description
  * publications
