package example.awsLambdaPoc.service;

import org.postgresql.copy.CopyManager;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;

public class CsvService {


    private final S3Client s3Client = S3Client.builder().build();

    private static final String POSTGRES_URL = System.getenv("POSTGRES_URL");
    private static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
    private static final String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");
    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");


    public String process() throws SQLException {
        System.out.println("Inside process() method");
        String bucketName = BUCKET_NAME;
        String sqlKey = System.getenv("DDL_KEY");
        System.out.println("sqlKey: " + sqlKey);
        String csvKey = System.getenv("CSV_KEY");
        System.out.println("csvKey: " + csvKey);
        S3Client s3Client = S3Client.builder().build();
        try (Connection conn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)) {
                conn.setAutoCommit(false);
                System.out.println("Connected to PostgreSQL");
                //Execute DDL
                InputStream ddlStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(sqlKey).build());
                System.out.println("Executing ddlStream");
                executeDDL(ddlStream, conn);
                System.out.println(conn.getMetaData().getURL());

                //Process CSV
                processCsvUsingCopy(bucketName, csvKey, conn);
                conn.commit();
                return "Success";

        } catch (IOException e) {
            return "Failed: " + e.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private void executeDDL(InputStream ddlStream, Connection conn) throws SQLException, IOException {
        System.out.println("Inside ddlStream");
        try {
            if (tableExists(conn, "addressmig_green.dpa")) {
                return;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        BufferedReader sqlReader = new BufferedReader(new InputStreamReader(ddlStream));
        Statement statement = conn.createStatement();
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = sqlReader.readLine()) != null) {
            stringBuilder.append(line);
        }
        statement.execute(stringBuilder.toString());
        conn.commit();
        System.out.println("Done DDL");
    }

    private boolean tableExists(Connection conn, String s) {
        System.out.println("Inside tableExists() method");
        try (ResultSet rs = conn.getMetaData().getTables(null, null, "addressmig_green.dpa", null)) {
            return rs.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private String processCsvUsingCopy(String bucket, String key, Connection conn) throws Exception {

        System.out.println("Starting COPY...");

        // Get file from S3
        ResponseInputStream<GetObjectResponse> s3Object =
                s3Client.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build());

        BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object));
        // Skip header
        reader.readLine();
        CopyManager copyManager = new CopyManager(conn.unwrap(org.postgresql.core.BaseConnection.class));
        String copySql = """
        COPY addressmig_green.dpa(
            uprn,
            organisation_name,
            department_name,
            sub_building_name,
            building_name,
            dependent_thoroughfare,
            thoroughfare,
            double_dependent_locality,
            dependent_locality,
            post_town,
            postcode,
            postcode_type,
            delivery_point_suffix,
            entry_date
        )
        FROM STDIN WITH (
            FORMAT csv,
            HEADER false,
            DELIMITER ',',
            QUOTE '"',
            NULL ''
        )
    """;
        try{
            long rowsInserted = copyManager.copyIn(copySql, reader);
            System.out.println("Inserted rows: " + rowsInserted);
            return "Inserted " + rowsInserted + " rows";
        } catch (Exception e) {
            System.out.println("Error copying: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public String testConnection() {
        try {
            String POSTGRES_URL = System.getenv("POSTGRES_URL");
            String POSTGRES_USER = System.getenv("POSTGRES_USER");
            String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");

            Connection conn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
            if (conn != null && !conn.isClosed()) {
                return "Connection Successful";
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return "Connection Failed" + e.getMessage();
        }
        return "Unknown";
    }
}
