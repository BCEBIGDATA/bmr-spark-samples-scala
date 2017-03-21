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

object AccessLogStatsScalaSample {
    private val parser = new LogParser
    private val logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US)
    private val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    def fetchDate(GMSTime: String): String = {
        simpleDateFormat.format(logDateFormat.parse(GMSTime))
    }

    def main(args: Array[String]) {

        if (args.length != 1) {
            System.err.println("usage: spark-submit com.baidubce.bmr.sample.AccessLogStatsScalaSample <input>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("AccessLogStatsScalaSample")
        val sc = new SparkContext(sparkConf)

        // parse the log to LogRecord and cache
        val distFile = sc.textFile(args(0)).map(parser.parseRecord(_)).cache()

        /*
         * change the LogRecord to (date, 1) format, and caculate each day's page view
         */
        println("------PV------")
        val PV = distFile.map(s => (fetchDate(s.timeLocal), 1)).reduceByKey(_ + _)
        // for large dataset, use collect() may cause out of memory error
        PV.collect().foreach(println)  // print PV by day in driver

        /*
         * change the LogRecord to (date, remoteAddr) format,
         * and group the keys to aggregate the remoteAddr.
         * We change the grouped Iteratable Type to Set for uniqueness.
         * Finally we can calculate the number of Unique Visitors
         */
        println("------UV------")
        val UV = distFile.map(s => (fetchDate(s.timeLocal), s.remoteAddr))
                         .groupByKey().map(s => (s._1, s._2.toSet.size))
        // for large dataset, use collect() may cause out of memory error
        UV.collect().foreach(println)  // print UV by day in driver
    }
}



