CREATE SCHEMA IF NOT EXISTS lambda_demo;
CREATE TABLE IF NOT EXISTS lambda_demo.dpa
(uprn VARCHAR NOT NULL,
    organisation_name VARCHAR(60) ,
    department_name VARCHAR(60) ,
    sub_building_name VARCHAR(30) ,
    dependent_thoroughfare VARCHAR(80) ,
    thoroughfare VARCHAR(80) ,
    double_dependent_locality VARCHAR(35) ,
    dependent_locality VARCHAR(35) ,
    post_town VARCHAR(30) ,
    postcode VARCHAR(8) ,
    postcode_type VARCHAR(1) ,
    delivery_point_suffix VARCHAR(2) ,
    entry_date DATE
    );