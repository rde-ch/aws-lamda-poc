package example.awsLambdaPoc.util;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

public class LambdaUtility {
    private final S3Client s3Client = S3Client.builder().build();
    private String processCsvFromS3(String bucket, String csvKey, Connection conn) throws IOException {

        System.out.println("Inside processCsvFromS3() method");
        int count = 0;
        String line = "";
        boolean isFirstLine = true;
        try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder().bucket(bucket)
                .key(csvKey).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
            System.out.println("Executing s3Object");
            String sql = "INSERT INTO addressmig_green.dpa( uprn, udprn, organisation_name, department_name, sub_building_name, " +
                    "building_name, building_number, dependent_thoroughfare, thoroughfare, double_dependent_locality, dependent_locality, " +
                    "post_town, postcode, postcode_type, delivery_point_suffix, po_box_number, entry_date)\n" +
                    "\tVALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            System.out.println("Before Prepared");
            PreparedStatement stmt = conn.prepareStatement(sql);
            System.out.println("Executing Prepared");
            while ((line = reader.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                CSVReader csvReader = new CSVReader(new InputStreamReader(s3Object));
                String[] datas;
                while((datas = csvReader.readNext()) != null) {
                    System.out.println("Datas " + datas.length);
                    setPreparedStatementValues(stmt, datas);
                    stmt.addBatch();
                }
                System.out.println("Done prepared statement");
                count++;
                if (count % 1000 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();
            System.out.println("Done executing batch");
            conn.commit();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return "Inserted " + count + " records successfully";
    }

    private static void setPreparedStatementValues(PreparedStatement stmt, String[] datas)
            throws SQLException {
        //  System.out.println("Inside setPreparedStatementValues method");
        for (int i = 0; i < datas.length; i++) {
            String value = datas[i];
            if (value == null || value.isEmpty()) {
                stmt.setNull(i + 1, Types.NULL);
                continue;
            }
            if (value.matches("\\d+")) {
                long num = Long.parseLong(value);
                if (num <= Integer.MAX_VALUE) {
                    stmt.setInt(i + 1, (int) num);
                } else {
                    stmt.setLong(i + 1, num);
                }
            } else if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
                stmt.setObject(i + 1, LocalDate.parse(value));
            } else {
                stmt.setString(i + 1, value);
            }
        }
    }

}
