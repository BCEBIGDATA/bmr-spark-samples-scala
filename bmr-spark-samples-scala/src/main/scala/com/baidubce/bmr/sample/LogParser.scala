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
import java.util.regex.Matcher
import java.util.regex.Pattern

case class LogRecord(remoteAddr: String, timeLocal: String, request: String, status: String,
                           bodyBytesSent: String, httpReferer: String, httpCookie: String, 
                           remoteUser: String, httpUserAgent: String, host: String, msec: String)

@SerialVersionUID(100L)
class LogParser extends Serializable {
    private val ipAddr = "(\\S+)" // like `123.456.7.89`
    private val remoteUser = "(.*?)"
    private val timeLocal = "(.*?)" // like `[21/Jul/2009:02:48:13 -0700]`
    private val request = "(.*?)" // any number of any character, reluctant
    private val status = "(\\d{3})"
    private val bodyBytesSent = "(\\S+)" // this can be a "-"
    private val httpReferer = "(.*?)"
    private val httpCookie = "(.*?)"
    private val requestTime = "(\\S+)"
    private val httpUserAgent = "(.*?)"
    private val host = "(\\S+)"
    private val msec = "(\\S+)"

    private val regex = s"""$ipAddr\\s+-\\s+\\[$timeLocal\\]\\s+\"$request\"\\s+$status\\s+$bodyBytesSent\\s+\"$httpReferer\"\\s+""" +
    s"""\"$httpCookie\"\\s+$remoteUser\\s+\"$httpUserAgent\"\\s+$requestTime\\s+$host\\s+$msec"""
    
    private val p = Pattern.compile(regex)

    def parseRecord(record: String): LogRecord = {
        val matcher = p.matcher(record)
        if (matcher.find) {
            buildLogRecord(matcher)
        } else {
            // here you can print the records to see the unmatched ones.
            LogRecord("", "", "", "", "", "", "", "", "", "", "")
        }
    }

    def buildLogRecord(matcher: Matcher) = {
        LogRecord(
            matcher.group(1),
            matcher.group(2),
            matcher.group(3),
            matcher.group(4),
            matcher.group(5),
            matcher.group(6),
            matcher.group(7),
            matcher.group(8),
            matcher.group(9),
            matcher.group(10),
            matcher.group(11))
    }
}
