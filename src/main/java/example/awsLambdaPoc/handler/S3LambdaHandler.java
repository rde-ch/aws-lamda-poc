package example.awsLambdaPoc.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import example.awsLambdaPoc.service.CsvService;

import java.sql.SQLException;

public class S3LambdaHandler implements RequestHandler<Object, String>{
    private final CsvService csvService = new CsvService();

   @Override
    public String handleRequest(Object input, Context context) {
       System.out.println("Handler started");
       try {
           return csvService.process();
       } catch (SQLException e) {
           e.printStackTrace();
           return "Failed" + e.getMessage();
       }
   }
}
