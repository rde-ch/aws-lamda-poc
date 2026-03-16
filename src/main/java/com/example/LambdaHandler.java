package com.example;

import com.amazonaws.lambda.core.Context;
import com.amazonaws.lambda.core.RequestHandler;

public class LambdaHandler implements RequestHandler<String, String> {
    @Override
    public String handleRequest(String input, Context context) {
        context.getLogger().log("Input: " + input);
        return "Hello from AWS Lambda! Input was: " + input;
    }
}