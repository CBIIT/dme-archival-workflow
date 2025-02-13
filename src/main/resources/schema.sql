CREATE TABLE DME_WFLOW_DB.COLLECTION_NAME_MAPPING
(
    ID              NUMBER(18) PRIMARY KEY,
    DOC             VARCHAR(255),
    COLLECTION_TYPE VARCHAR(255),
    MAP_KEY         VARCHAR(255),
    MAP_VALUE       VARCHAR(255)
);
--) TABLESPACE dmewflowdev;

CREATE TABLE DME_WFLOW_DB.METADATA_INFO
(
    ID              NUMBER(18) PRIMARY KEY,
    META_DATA_KEY   VARCHAR(255),
    META_DATA_VALUE VARCHAR(2700),
    OBJECT_ID       NUMBER(18)
);
--) TABLESPACE dmewflowdev;

CREATE TABLE DME_WFLOW_DB.STATUS_INFO
(
    ID                     NUMBER(18) PRIMARY KEY,
    CHECKSUM               VARCHAR(255),
    ERROR                  CLOB,
    FILESIZE               NUMBER(18),
    FULL_DESTINATION_PATH  VARCHAR(255),
    ORGINAL_FILE_NAME      VARCHAR(255),
    ORIGINAL_FILE_PATH     VARCHAR(255),
    RUN_ID                 VARCHAR(255),
    SOURCE_FILE_NAME       VARCHAR(255),
    SOURCE_FILE_PATH       VARCHAR(255),
    STATUS                 VARCHAR(255),
    START_TIMESTAMP        TIMESTAMP,
    END_TIMESTAMP          TIMESTAMP,
    UPLOAD_START_TIMESTAMP TIMESTAMP,
    UPLOAD_END_TIMESTAMP   TIMESTAMP,
    TAR_START_TIMESTAMP    TIMESTAMP,
    TAR_END_TIMESTAMP      TIMESTAMP,
    TAR_INDEX_START        TIMESTAMP,
    TAR_INDEX_END          TIMESTAMP,
    RETRY_COUNT            NUMBER(10),
    DOC                    VARCHAR(255),
    TAR_CONTENTS_COUNT     NUMBER(10)
);
--) TABLESPACE dmewflowdev;

CREATE TABLE DME_WFLOW_DB.TASK_INFO
(
    ID        NUMBER(18) PRIMARY KEY,
    OBJECT_ID NUMBER(18),
    TASK_NAME VARCHAR(255),
    COMPLETED VARCHAR(255)
);
--) TABLESPACE dmewflowdev;

CREATE TABLE DME_WFLOW_DB.METADATA_MAPPING
(
    ID              NUMBER(18) PRIMARY KEY,
    DOC             VARCHAR(255),
    COLLECTION_NAME VARCHAR(255),
    COLLECTION_TYPE VARCHAR(255),
    META_DATA_KEY   VARCHAR(255),
    META_DATA_VALUE VARCHAR(2700)
);
--) TABLESPACE dmewflowdev;

CREATE TABLE DME_WFLOW_DB.PERMISSION_BOOKMARK_INFO
(
    ID              NUMBER(18) PRIMARY KEY,
    PATH            VARCHAR(255),
    USER_ID         VARCHAR(255),
    PERMISSION      VARCHAR(255),
    CREATE_BOOKMARK VARCHAR(255),
    CREATED         VARCHAR(255),
    ERROR           VARCHAR(1000)
);
--) TABLESPACE dmewflowdev;

create sequence STATUS_INFO_SEQ nocache;
create sequence METADATA_INFO_SEQ nocache;
create sequence TASK_INFO_SEQ nocache;
ALTER SEQUENCE STATUS_INFO_SEQ INCREMENT BY 100000;
select STATUS_INFO_SEQ.nextval from dual;
ALTER SEQUENCE STATUS_INFO_SEQ INCREMENT BY 1;