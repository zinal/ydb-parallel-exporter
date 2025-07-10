# YDB parallel records batch processor

[See the Releases page for downloads](https://github.com/zinal/ydb-parallel-exporter/releases).

This tool provides basic capabilities for parallel job processing of records stored in [YDB](https://ydb.tech) tables.
It can be used as a standalone program, or embedded into the user application (which needs to be based on Java-compatible stack) as a library.

The basic workflow implemented by the tool consists of two phases:

1. The tool grabs the record keys using the initial query, applying the basic filtering. Output paging can optionally be used, as [described in the YDB documentation](https://ydb.tech/docs/en/dev/paging). For paging, the additional "paging" query needs to be configured.
2. The tool uses the collected record keys to apply the additional processing, with the following options:
  * Table joins and additional computation may be applied to grab and enrich the records, and output the full records in CSV or JSON format.
  * Data records can be updated, such as setting some fields in the existing records or inserting new records.

Phase 2 is executed using the configurable executor pool, providing parallel execution for enhanced performance. This means that the output records cannot generally be sorted, as the output from different parallel jobs will be provided in the undefined order.

## Running the tool as a standalone program

```bash
./Run.sh connection.xml jobdef.xml
... or ...
./Run.sh connection.xml jobdef.xml vars.xml
```

* The first parameter should point to the file with the connection parameters.
* The second parameter should point to the file with the job definition.
* The optional third parameter can be missing, or should point to the file with the substitution variables.

For information about the substitution variables, see the section at the end of the README file.

## Embedding the tool into the user program

Using the class `tech.ydb.samples.exporter.Tool`, the following can be implemented:

```java
JobDef job = new JobDef();
job.setMainQuery("SELECT ...");
job.setDetailsQuery("SELECT ...");
...
Properties propsConn = new Properties();
propsConn.setProperty("ydb.url", "grpcs://ydb01.localdomain:2135/cluster1/testdb");
...
try (YdbConnector yc = new YdbConnector(propsConn)) {
    try (Tool app = new Tool(yc, job)) {
        app.run();
    }
}
```

## Connection parameters

Connection parameters are provided either programmatically (as `java.util.Properties` object), or via XML properties file.

| **Parameter** | **Description** |
| --- | --- |
| `ydb.url` | YDB connection URL |
| `ydb.cafile` | Path to the custom CA certificate file |
| `ydb.auth.mode` | [Authentication mode](https://ydb.tech/docs/en/reference/ydb-sdk/auth#auth-provider) (NONE, ENV, STATIC, METADATA, SAKEY) |
| `ydb.auth.username` | Username for STATIC authentication |
| `ydb.auth.password` | Password for STATIC authentication |
| `ydb.auth.sakey` | Service account key file name for SAKEY authentication |
| `ydb.preferLocalDc` | Prefer local or nearest datacenter for connections (true or false, default false) |

Example properies file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<entry key="ydb.url">grpcs://ydb01.localdomain:2135/cluster1/testdb</entry>
<entry key="ydb.cafile">/home/ydbadmin/Work/cluster1/tls/ca.crt</entry>
<entry key="ydb.auth.mode">STATIC</entry>
<entry key="ydb.auth.username">root</entry>
<entry key="ydb.auth.password">???</entry>
<entry key="ydb.poolSize">1000</entry>
</properties>
```

## Processing parameters

Processing parameters are provieded either programmatically (as `tech.ydb.samples.exporter.JobDef` object), or via XML configuration file.

| **Parameter** | **Description** |
| --- | --- |
| `worker-count` | Number of parallel workers |
| `queue-size` | Size of the queue between the main query and the detail query |
| `batch-limit` | Maximum number of keys in a batch for details query |
| `output-format` | Output format (CSV, TSV, JSON) |
| `output-file` | Output file name, with value '-' for STDOUT |
| `isolation` | Transaction isolation level (SERIALIZABLE_RW, SNAPSHOT_RO, STALE_RO, ONLINE_RO, ONLINE_INCONSISTENT_RO) |
| `timeout` | Query timeout, in milliseconds, for query-main, query-page or query-detail, default -1 (unlimited) |
| `query-main` | Main query (executed first) |
| `query-page` | Paging query (optional). Requires key sorting and row count limit both for itself and for the main query. |
| `query-detail` | Detail query. Takes the keys from main and page queries, and applies extra logic. |
| `input-page` | List of input columns for the page query (subset of columns from main and page queries) |
| `input-detail` | List of input columns for the detail query (subset of columns from main and page queries) |
| `output-page` | List of output columns for the page query |
| `output-detail` | List of output columns for the detail query |

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-parallel-exporter>
    <worker-count>5</worker-count>
    <queue-size>100</queue-size>
    <batch-limit>1000</batch-limit>
    <output-format>CSV</output-format>
    <output-file>example1.csv</output-file>
    <isolation>SNAPSHOT_RO</isolation>
    <query-main timeout="3000"><![CDATA[
SELECT sys_update_tv, id
FROM my_documents VIEW ix_sys_update_tv
WHERE sys_update_tv >= Timestamp('2021-01-01T00:00:00Z')
  AND sys_update_tv  < Timestamp('2021-01-02T00:00:00Z')
ORDER BY sys_update_tv, id   -- Mandatory sorting on primary key or secondary index
LIMIT 1000;   -- Mandatory limit on the number of output records
]]> </query-main>

    <!-- Колонки для параметров страничного запроса -->
    <input-page>
        <column-name>sys_update_tv</column-name>
        <column-name>id</column-name>
    </input-page>
    <query-page timeout="3000"><![CDATA[
DECLARE $input AS Struct<sys_update_tv:Timestamp?, id:Text>;
SELECT sys_update_tv, id
FROM my_documents VIEW ix_sys_update_tv
WHERE (sys_update_tv, id) > ($input.sys_update_tv, $input.id) -- Paging condition
  AND sys_update_tv < Timestamp('2021-01-02T00:00:00Z') -- Repeat the filter from the main query
ORDER BY sys_update_tv, id   -- Mandatory sorting on primary key or secondary index
LIMIT 1000;   -- Mandatory limit on the number of output records
]]> </query-page>
    <input-details timeout="10000">
        <column-name>id</column-name>
    </input-details>
    <query-details><![CDATA[
DECLARE $input AS List<Struct<id:Text>>;
SELECT
    documents.*,
    d1.attr1 AS d1_attr1,
    d2.attr1 AS d2_attr1
FROM AS_TABLE($input) AS input
INNER JOIN my_documents VIEW PRIMARY KEY AS documents
  ON input.id=documents.id
LEFT JOIN my_dict1 AS d1
  ON d1.key=documents.dict1
LEFT JOIN my_dict2 AS d2
  ON d2.key=documents.dict2
WHERE documents.some_state IN ('ONE'u, 'TWO'u, 'THREE'u, 'FOUR'u)
  AND documents.input_tv IS NOT NULL;
]]> </query-details>
</ydb-parallel-exporter>
```

## Substitution variables

The tool can optionally apply variable substitutions to the XML file with job definition.

Substitutions are applied after parsing the XML content (so the file needs to be a valid XML before substititions), but before converting it to the job definition.

The variables can be specified as `${varname}` in attribute values, text values and CDATA section values, but not in the tag names or attribute names.

Example properies file containing some substitution variables:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<entry key="file_name">output_with_some_suffix.csv</entry>
<entry key="worker_count">10</entry>
<entry key="start_time">2020-01-01T00:00:00Z</entry>
<entry key="finish_time">2026-01-01T00:00:00Z</entry>
</properties>
```

## Usage examples

The YDB parallel exporter tool can be used for various data processing tasks. Below are examples for common use cases:

### 1. Filling a new column with computed data

This example shows how to fill a new column in a table with data computed from other columns using SQL queries.

Suppose that the table `some_table` has got a new column called `new_field` using the following statement:

```SQL
ALTER TABLE some_table ADD COLUMN new_field Text;
```

The `new_field` column contains just nulls, and there is a requirement that it should be filled with some data computed from the other fields (and possibly by requesting the data from other tables as well).

**Job definition file (`fill-column.xml`):**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-parallel-exporter>
    <worker-count>10</worker-count>
    <queue-size>100</queue-size>
    <batch-limit>1000</batch-limit>
    <output-format>TSV</output-format>
    <output-file>-</output-file>
    <isolation>SERIALIZABLE_RW</isolation>

    <!-- Select records that need the new column filled -->
    <query-main timeout="120000"><![CDATA[
SELECT id FROM some_table
WHERE new_field IS NULL
ORDER BY id;
]]> </query-main>

    <input-details>
        <column-name>id</column-name>
    </input-details>

    <!-- Update the new column with computed data -->
    <query-details timeout="10000"><![CDATA[
DECLARE $input AS List<Struct<id:Text>>;
UPSERT INTO some_table
  SELECT i.id AS id,
      t.old_field || ' 'u || a.some_name AS new_field
  FROM AS_TABLE($input) AS i
  JOIN some_table AS t
    ON t.id = i.id
  LEFT JOIN another_table AS a
    ON a.id = t.ref_a;
]]> </query-details>
</ydb-parallel-exporter>
```

**Execution:**
```bash
./Run.sh connection.xml fill-column.xml
```

### 2. Archiving old records to another table

This example demonstrates archiving older records to an archive table and then deleting them from the original table.

Suppose that the original table `documents` has to be cleared every month from the documents older that 3 months.
The old documents should be put into the `documents_archive` table, and deleted from the original `documents` table.

**Job definition file (`archive-records.xml`):**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-parallel-exporter>
    <worker-count>5</worker-count>
    <queue-size>100</queue-size>
    <batch-limit>1000</batch-limit>
    <output-format>TSV</output-format>
    <output-file>-</output-file>
    <isolation>SERIALIZABLE_RW</isolation>

    <!-- Select records to archive (older than 1 year) -->
    <query-main timeout="120000"><![CDATA[
SELECT id FROM documents
WHERE created_date < CurrentUtcTimestamp() - Interval('P92D');
]]> </query-main>

    <input-details>
        <column-name>id</column-name>
    </input-details>

    <!-- Archive records and then delete from original table -->
    <query-details timeout="10000"><![CDATA[
DECLARE $input AS List<Struct<id:Text>>;

-- First, insert into archive table
UPSERT INTO documents_archive
SELECT d.*
FROM AS_TABLE($input) AS i
JOIN documents AS d ON d.id = i.id;

-- Then, delete from original table
DELETE FROM documents
ON SELECT * FROM AS_TABLE($input);
]]> </query-details>
</ydb-parallel-exporter>
```

**Execution:**
```bash
./Run.sh connection.xml archive-records.xml
```

### 3. Extracting large amounts of data to CSV files

This example shows how to extract large datasets to CSV files with parallel processing and paging for better performance.

Suppose that there is a huge amount of data stored in the normalized table structure, and the BI system needs the de-normalized data to fill the data mart. Every day a job is run to extract the last two days' data into the CSV file in the form of a wide table, containing all the required attritutes to fill the data mart. The file is then loaded into the BI system using its native tools.

Job parameters are put into the separate substitution variables file, which is re-generated every time before job run.

**Job substitution variable file (`export-to-csv_params.xml`):**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<entry key="file_name">exported_data.csv</entry>
<entry key="worker_count">10</entry>
<entry key="start_time">2020-01-01T00:00:00Z</entry>
<entry key="finish_time">2026-01-01T00:00:00Z</entry>
</properties>
```

**Job definition file (`export-to-csv.xml`):**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-parallel-exporter>
    <worker-count>${worker_count}</worker-count>
    <queue-size>100</queue-size>
    <batch-limit>1000</batch-limit>
    <output-format>CSV</output-format>
    <output-file>${file_name}</output-file>
    <output-encoding>UTF-8</output-encoding>
    <isolation>SNAPSHOT_RO</isolation>

    <!-- Main query with paging for large datasets -->
    <query-main timeout="5000"><![CDATA[
SELECT sys_update_tv, id
FROM my_documents VIEW ix_sys_update_tv
WHERE sys_update_tv >= Timestamp('${start_time}')
  AND sys_update_tv < Timestamp('${finish_time}')
ORDER BY sys_update_tv, id
LIMIT 1000;
]]> </query-main>

    <!-- Paging query for handling large datasets -->
    <input-page>
        <column-name>sys_update_tv</column-name>
        <column-name>id</column-name>
    </input-page>
    <query-page timeout="5000"><![CDATA[
DECLARE $input AS Struct<sys_update_tv:Timestamp?, id:Text>;
SELECT sys_update_tv, id
FROM my_documents VIEW ix_sys_update_tv
WHERE (sys_update_tv, id) > ($input.sys_update_tv, $input.id)
  AND sys_update_tv < Timestamp('${finish_time}')
ORDER BY sys_update_tv, id
LIMIT 1000;
]]> </query-page>

    <!-- Detail query with joins for enriched data and extra filters -->
    <input-details>
        <column-name>id</column-name>
    </input-details>
    <query-details timeout="5000"><![CDATA[
DECLARE $input AS List<Struct<id:Text>>;
SELECT
    documents.*,
    d1.attr1 AS d1_attr1,
    d2.attr1 AS d2_attr1
FROM AS_TABLE($input) AS input
INNER JOIN my_documents VIEW PRIMARY KEY AS documents
  ON input.id=documents.id
LEFT JOIN my_dict1 AS d1
  ON d1.key=documents.dict1
LEFT JOIN my_dict2 AS d2
  ON d2.key=documents.dict2
WHERE documents.some_state IN ('ONE'u, 'TWO'u, 'THREE'u, 'FOUR'u)
  AND documents.input_tv IS NOT NULL;
]]> </query-details>
</ydb-parallel-exporter>
```

**Execution:**
```bash
./Run.sh connection.xml export-to-csv.xml export-to-csv_params.xml
```
