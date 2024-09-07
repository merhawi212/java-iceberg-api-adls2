package com.merasdecode.iceberg_api;

import lombok.var;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.IntStream;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String tableName = "users";
        String schemaName = "raw";
        String catalogName ="adls2";
        LOGGER.info("Started");

        Properties properties = new Properties();
        var envFile = Paths.get("src/main/resources/config.properties");
        try (var inputStream = Files.newInputStream(envFile)) {
            properties.load(inputStream);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
        String accountKey =  properties.get("account.key").toString();
        String storageAccountName =  properties.get("storage.account.name").toString();
        String containerName = properties.get("container.name").toString();
        String thriftURL = properties.get("thrift.url").toString();
        String baseURL = String.format("abfss://%s@%s.dfs.core.windows.net", containerName, storageAccountName);


        HiveCatalog hiveCatalog = getHiveCatalog(catalogName, baseURL, storageAccountName, accountKey, thriftURL);
        // Define the schema for the table
        Schema schema = new Schema(
                Types.NestedField.required(1, "name", Types.StringType.get()),
                Types.NestedField.optional(2, "email", Types.StringType.get()),
                Types.NestedField.optional(3, "dob", Types.DateType.get())
        );
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(schemaName), tableName);

        hiveCatalog.createTable(tableIdentifier, schema); // Create the table
        Table table = hiveCatalog.loadTable(tableIdentifier);
        LOGGER.info("table name: {}", table.name());
        LOGGER.info("table location: {}", table.location()); // table location

//        hiveCatalog.dropTable(tableIdentifier); // drop table

        var records = createGenericRecords((schema));
//        records.forEach(record -> System.out.println(record.toString())); // Display generated records
        try{
//              write(table, schema, records); // write to iceberg table
               LOGGER.info("Read data from iceberg table: {}", table.name());
//              read(table); // read from iceberg table
        }catch (Exception ex){
            LOGGER.error(ex.getMessage(), ex);
        }
    }
    private static List<GenericRecord> createGenericRecords(Schema schema){
        List<GenericRecord> records = new ArrayList<>();

        // Sample data
        String[] names  = {"Habtom Berhe", "John Smith", "Semhar  Tedros", "Fishale Gebrehiwet", "Eyasu Kiflay"};
        String[] emails  = {"habtomberhe@gmail.com", "smith.john@gmail.com","semhar.tedros@gmail.com", "fishgebre@gmail.com", "eyasu@gmail.com"};
        String[] dob  = {"1996-12-05", "1996-11-25", "1997-01-09", "1998-10-03", "1998-11-04"};

        IntStream.range(0, names.length).forEach(i ->{
                GenericRecord record = GenericRecord.create(schema);
                record.setField("name", names[i]); // Assumes schema has "name" field
                record.setField("email", emails[i]); // Assumes schema has "email" field
                record.setField("dob", LocalDate.parse(dob[i])); // Assumes schema has "dob" field
                records.add(record);
        });
        return records;
    }

    private static void write(Table table, Schema schema, List<GenericRecord> records) throws IOException {
        String  randomFileName = UUID.randomUUID().toString();
        String fileExtension = ".parquet";
        String dataFolder = "/data";

        String filePath = table.location() + dataFolder + randomFileName + fileExtension;
        OutputFile file = table.io().newOutputFile(filePath);

        org.apache.iceberg.io.DataWriter<GenericRecord> dataWriter = Parquet.writeData(file).schema(schema)
                                                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                                                .overwrite().withSpec(PartitionSpec.unpartitioned())
                                                                .build();
        for (GenericRecord record : records)
            dataWriter.write(record);
        dataWriter.close();
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }
    private static void read(Table table){
        CloseableIterable<Record> result = IcebergGenerics.read(table).build();
        result.forEach(System.out::println);
    }

    private static HiveCatalog getHiveCatalog(String catalogName, String baseURL, String storageAccountName, String accountKey, String thriftURL) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("fs.defaultFS", baseURL);
        hiveConf.set(String.format("fs.azure.account.key.%s.dfs.core.windows.net", storageAccountName), accountKey);
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(hiveConf);
        hiveCatalog.initialize(catalogName, getCatalogProperties(storageAccountName, accountKey, baseURL, thriftURL));
        return hiveCatalog;
    }

    private static Map<String, String> getCatalogProperties(String storageAccountName, String accountKey, String baseURL, String thriftURL) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", baseURL);
        catalogProperties.put("uri", thriftURL);
        catalogProperties.put("io-imp", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
        catalogProperties.put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY, accountKey);
        catalogProperties.put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME, storageAccountName);
        return catalogProperties;
    }
}