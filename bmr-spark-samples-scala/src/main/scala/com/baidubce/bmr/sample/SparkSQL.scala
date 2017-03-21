/*
 * Copyright (C) 2015 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidubce.bmr.sample

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object AccessLogStatsSQLSample {
    private val parser = new LogParser
    private val logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US)
    private val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    case class Record(date: String, addr: String)

    def fetchDate(GMSTime: String): String = {
        simpleDateFormat.format(logDateFormat.parse(GMSTime))
    }

    def main(args: Array[String]) {

        if (args.length != 1) {
            System.err.println("usage: spark-submit com.baidubce.bmr.sample.AccessLogStatsSQLSample <input>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("AccessLogStatsSQLSample")
        val sc = new SparkContext(sparkConf)
        // A hive context adds support for finding tables in the MetaStore and writing queries
        // using HiveQL. Users who do not have an existing Hive deployment can still create a
        // HiveContext. When not configured by the hive-site.xml, the context automatically
        // creates metastore_db and warehouse in the current directory.
        val hiveContext = new HiveContext(sc)
        import hiveContext.implicits._
        import hiveContext.sql

        // parse the log to LogRecord and cache
        
        val fileDF = sc.textFile(args(0)).map(parser.parseRecord(_))
                        .map(s => Record(fetchDate(s.timeLocal), s.remoteAddr)).toDF()
        fileDF.registerTempTable("temp_table")

        println("------PV------")
        sql("select date, count(*) as pv from temp_table group by date").show()

        println("------UV------")
        sql("select date, count(distinct addr) as uv from temp_table group by date").show()

    }
}

