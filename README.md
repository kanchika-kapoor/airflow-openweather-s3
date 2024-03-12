# OpenWeather API DAG for airflow

This DAG takes data from openweather api, normalizes and stores it in csv which is moved to S3 bucket.
The credentials are stored in airflow variables and http connection is made to openweather api through airflow connection configuration.

#App Screens

![Weather DAG]( ./images/weatherDAG.png?raw=true "DAG")

![S3 Bucket]( ./images/s3_screenshot.png?raw=true "s3 bucket")