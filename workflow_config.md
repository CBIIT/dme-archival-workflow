# Workflow Configuration Options

**The workflow can be customized using flexible configuration options available.** These options include the source 
path where the data shall be picked up from, whether any pre-processing is required such as tarring the folder, 
whether any patterns should be applied to include and exclude some files/folders,
and whether to look for a specific file to indicate it is ready to be picked up.


### List of Configuration Options Available

* Source directory to scan for data to be uploaded
* Temporary directory to be used if any preprocessing is required, such as tar/untar and compression
* Option to tar the folder prior to upload
* Comma-separated list of folders to be excluded from the tar files.
* Option to untar a file prior to upload
* Option to compress the file prior to upload.
* Option to tar individual files prior to upload to retain file information.
* Comma-separated patterns to exclude files from archival.
* Comma-separated patterns to include files from archival.
* Option to check for last modified date to see if a specified number of days has passed before uploading to DME.
* Option to check for existence of a file with the specified extension before uploading to DME.
* Option to read in metadata from an external file.
