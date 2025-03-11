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

On your local machine, perfrom the following to tunnel port 8888.
```
$ ssh -L 8887:localhost:8888 user@remotemachine
```

Now the default app page can be accessed from your local browser:
```
http://localhost:8887/home
```

## Customizing the Workflow

### Configuration Parameters

The following properties can be set in `application.properties` file:

* `dmesync.source.base.dir=<dir>`
  * The app will scan the directory specified
* `dmesync.source.base.dir.folders=<comma separeted list of folders>`
  * If specified, app will only scan these folders under the base dir.
* `dmesync.work.base.dir=<dir>`
  * The app will use this as a working directory for tar/untar and compression
* `dmesync.destination.base.dir=<collection>`
  * The base collection path in DME
* `dmesync.doc.name=[hitif|cmm|default]`
  * The doc name used for the custom business logic to build DME path, collection metadata and metadata for the object.
  * Default: `default`
* `dmesync.noscan.rerun=[true|false]`
  * If `true`, instead of scanning for files under the base dir, it will reprocess files from the database.
  * Default: `false`
* `dmesync.tar=[true|false]`
  * If `true`, it will tar the collection specified by `dmesync.preprocess.depth` from `dmesync.source.base.dir`.
  * Default: `false` 
* `dmesync.tar.exclude.folder=[folder1,folder2]`
  * If specified, folders that matches the folder names in a comma separated list will be excluded from the tar ball. 
* `dmesync.untar=[true|false]`
  * If `true`, it will untar the collection specified by `dmesync.preprocess.depth` from `dmesync.source.base.dir`.
  * Default: `false` 
* `dmesync.compress=[true|false]`
  * If `true`, the data object will be compressed prior to upload.
  * Default: `false` 
* `dmesync.preprocess.depth=[1,2,...|-1]`
  * If `dmesync.tar` or `dmesync.untar` is true, `dmesync.preprocess.depth` will be used to determine the collection 
  which requires tar or untar.
  * If -1 is specified with tar option, it will tar the leaf folder only.
  * For example:
     ```
    dmesync.source.base.dir = /home/user/instrument
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
  
* `dmesync.exclude.pattern=[glob]`
  * If specified, object with the specified pattern will be excluded.
  * Comma-separated multiple patterns can be specified.
  * If both include and exclude pattern is applicable for a file/folder, the file/folder will be excluded as the 
  exclusion takes precedence.
  * For example:
     ```
     **/pi_b/**
     - Will exclude any file/folders which has pi_b folder in the path.
     ```
* `dmesync.include.pattern=[glob]`
  * If specified, only object with the specified pattern will be include.
  * Comma-separated multiple patterns can be specified.
  * For example:
     ```
     **/pi_a/**
     - Will include file/folders which has pi_a folder in the path.
     instrument/**/project1/**
     - Will include file/folders which starts with instrument folder directly under the base dir, and has project1 folder after any parent folder.
     ```
* `dmesync.dryrun=[true|false]`
  * If true, only records the files to be processed in the local DB without running the workflow.
  * Default: `false` 
* `dmesync.cleanup=[true|false]`
  * If true, the tar file created under the dmesync.work.base.dir will be removed upon successful upload.
  * Default: `false` 
* `dmesync.verify.prev.upload=[none|local]`
  * If `none`, it does not check whether it has previously been uploaded.
  * If `local`, it will check the local db if it has previously been uploaded, and skip the file.
  * Default: `none`
* `dmesync.cron.expression=[cron expression]` 
  * **This expression is not used if `dmesync.run.once.and.shutdown` flag is `true`**
  * For example:
    ```
    0 0/5 * * * ? //every 5 minutes
    0 0 0 1 1 ? //Jan 1st of the year
    format: Sec Min Hour Day Mon SUN-SUN:0-7
    ```
* `dmesync.run.once.and.shutdown=[true|false]`
  * **Also see `dmesync.run.once.run_id`**
  * If `true`, once the run has completed regardless of any failures, the application will shutdown.
  * Default: `false` 
* `dmesync.run.once.run_id=<run id>`
  * If `dmesync.run.once.and.shutdown=true`, the user **must** supply a unique run id for this run.
  * Recommended run id example: `Run_YYYYMMDDHHMISS`
* `dmesync.last.modified.days=[1,2,...]`
  * If specified, if modified date of the file/folder is within the number of days specified, it will not be archived.
* `dmesync.last.modified.under.basedir=[true|false]`
  * If specified, checks modified date of the folder in the base directory instead of folder/file
* `dmesync.last.modified.under.basedir.depth=[1,2,...]`
  * If specified, it will check for the last modified folder under the specified depth from the basedir.
* `dmesync.replace.modified.files=[true|false]`
  * If `true`, the system will compare the modified date against the last uploaded and reupload if modified.
* `dmesync.tar.file.exist=<filename>`
  * If specified, it will check whether a file with the specified file name exists before tar operation is performed.
* `dmesync.tar.file.exist.ext=<ext>`
  * If specified, it will check whether a file with the specified file extension exists before tar operation is performed.
* `dmesync.file.exist.under.basedir=[true|false]`
  * Check if the marker file specified in dmesync.tar.file.exist.ext is directly under the base directory.
* `dmesync.file.exist.under.basedir.depth=[1,2,...]`
  * If specified, it will check for the file under the specified depth from the basedir.
* `dmesync.tar.filename.excel.exist=[true|false]`
  * If specified, it will retrieve the tar name from the excel spreadsheet.
* `dmesync.tar.contents.file=[true|false]`
  * If specified, it will create the content file which includes files in the each tar and archive.

* `dmesync.process.multiple.tars=[true|false]`
  * If specified, it will create batch tars for the folder with the count specified by below property.
* `dmesync.multiple.tars.files.count=[50,..]`
  * If specified, it will create the tars for every specified batch files in the folder.
* `dmesync.multiple.tars.dir.folders=[<dir>]`
  * If specified, it will create the batch tars for the folders specified.


* `dmesync.admin.emails=<comma separated email addrresses>`
  * Once a run completes, the run result will be emailed to this address.

* `dmesync.additional.metadata.excel=<file path to the metadata file>`
  * If specified, application will load the custom metadata excel file supplied by the user.

* `spring.main.web-environment=[true|false]`
  * If `true`, enables the web environment.

Optionally, override system defaults for concurrent file processing with the following parameters.

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

| id | collection_type | map_key | map_value |
|---|---|---|---|
| 1 | PI | pi_a | PI_A_NAME |
| 2 | PI | pi_b | PI_B_NAME |

#### metadata_mapping table

| id | collection_type | collection_name | map_key | map_value |
|---|---|---|---|---|
| 1 | PI | PI_A_NAME | full_name | Jane Doe |
| 2 | PI | PI_A_NAME | email | someemail@address |

### Adding User permissions and bookmarks

This feature is only available if you are running the workflow as a **GROUP_ADMIN** role.
Once the collection/data objects are archived, if user permission (**READ, WRITE, OWN**) or bookmark needs to be added, 
it could be specified in the following table. The entries can be loaded by including it in the `data.sql` file 
for the metadata entries.

####  permission_bookmark_info table
| id | path | user_id | permission | create_bookmark | created | error |
|---|---|---|---|---|---|---|
| 1 | /DME/path1 | usera | READ | Y | "Y" if created | Error if any |
| 2 | /DME/path2 | userb | WRITE | N | "Y" if created | Error if any |

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
