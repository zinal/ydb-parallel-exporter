# YDB parallel records batch processor

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
```

* The first parameter should point to the file with the connection parameters.
* The second parameter should point to the file with the job definition.

## Embedding the tool into the user program

Using the class `tech.ydb.samples.exporter.Tool`, the following can be implemented:

```java
JobDef job = new JobDef();
job.setMainQuery("SELECT ...");
job.setDetailsQuery("SELECT ...");
...
Properties props = new Properties();
props.setProperty("ydb.url", "grpcs://ydb01.localdomain:2135/cluster1/testdb");
...
try (YdbConnector yc = new YdbConnector(props)) {
    try (Tool app = new Tool(yc, job)) {
        app.run();
    }
}
```

## Connection parameters

Connection parameters are provided either programmatically (as `java.util.Properties` object), or via XML properties file.

| **Parameter** | **Description** |
| --- | --- |
| ydb.url | YDB connection URL |
| ydb.cafile | Path to the custom CA certificate file |
| ydb.auth.mode | [Authentication mode](https://ydb.tech/docs/en/reference/ydb-sdk/auth#auth-provider) (NONE, ENV, STATIC, METADATA, SAKEY) |
| ydb.auth.username | Username for STATIC authentication |
| ydb.auth.password | Password for STATIC authentication |
| ydb.auth.sakey | Service account key file name for SAKEY authentication |

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
| worker-count | Number of parallel workers |
| queue-size | Size of the queue between the main query and the detail query |
| batch-limit | Maximum number of keys in a batch for details query |
| output-format | Output format (CSV, TSV, JSON) |
| output-file | Output file name, with value '-' for STDOUT |
| isolation | Transaction isolation level (SERIALIZABLE_RW, SNAPSHOT_RO, STALE_RO, ONLINE_RO, ONLINE_INCONSISTENT_RO) |
| query-main | Main query (executed first) |
| query-page | Paging query (optional). Requires key sorting and row count limit both for itself and for the main query. |
| query-detail | Detail query. Takes the keys from main and page queries, and applies extra logic. |
| input-page | List of input columns for the page query (subset of columns from main and page queries) |
| input-detail | List of input columns for the detail query (subset of columns from main and page queries) |
| output-page | List of output columns for the page query |
| output-detail | List of output columns for the detail query |

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-parallel-exporter>
    <worker-count>5</worker-count>
    <queue-size>100</queue-size>
    <batch-limit>1000</batch-limit>
    <output-format>CSV</output-format>
    <output-file>example1.csv</output-file>
    <isolation>SNAPSHOT_RO</isolation>
    <query-main><![CDATA[
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
    <query-page><![CDATA[
DECLARE $input AS Struct<sys_update_tv:Timestamp?, id:Text>;
SELECT sys_update_tv, id
FROM my_documents VIEW ix_sys_update_tv
WHERE (sys_update_tv, id) > ($input.sys_update_tv, $input.id) -- Paging condition
  AND sys_update_tv < Timestamp('2021-01-02T00:00:00Z') -- Repeat the filter from the main query
ORDER BY sys_update_tv, id   -- Mandatory sorting on primary key or secondary index
LIMIT 1000;   -- Mandatory limit on the number of output records
]]> </query-page>
    <input-details>
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
