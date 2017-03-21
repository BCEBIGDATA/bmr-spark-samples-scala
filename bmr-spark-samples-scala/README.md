Calculate PV and UV from access logs using spark scala and sql.

> make sure you have installed maven

## Check Style

    mvn scalastyle:check

## How to build

    mvn package

## How to run

    spark-submit --master yarn-cluster --class com.baidubce.bmr.sample.AccessLogStatsScalaSample ./target/bmr-spark-samples-1.0-SNAPSHOT.jar <input>

    spark-submit --master yarn-cluster --class com.baidubce.bmr.sample.AccessLogStatsSQLSample ./target/bmr-spark-samples-1.0-SNAPSHOT.jar <input>
