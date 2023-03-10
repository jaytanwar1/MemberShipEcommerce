# MemberShipEcommerce

The above code used to filter record from source file  with successfull and unsuccesfull record, which is assumed to present in yyyyMMddHH/data/*csv location.
It will be picked by corn job written in file otherSolution/corn_job_file.py which will run every hour.

For application logs config files can be passed at spark submit or can be added in spark-defaults. 

Add below lines in spark-defaults if don't want to add in spark submit : 
spark.driver.extraJavaOptions -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=membership-analysis 

The name of the log file will be membership-analysis.log , It will be created under app-logs. And the logs behaviour defined in log4j.properties
To run the project and test cases simple open the project in Intellij IDE preconfigured with pyspark, and pytest
