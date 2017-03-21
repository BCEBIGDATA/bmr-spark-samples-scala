本样例主要是引导大家怎么在BMR平台上使用Spark 和Spark SQL来分析Web日志，如统计每天的PV和UV等等。

###步骤1：输入数据准备 

样例中的数据是由Nginx产生的web访问日志,日志的格式设置如下：

    $remote_addr - [$time_local] "$request" $status $body_bytes_sent "$http_referer"  $http_cookie" $remote_user "$http_user_agent" $request_time  $host $msec

下面是一条具体日志：

    10.81.78.220 - [04/Oct/2015:21:31:22 +0800] "GET /u2bmp.html?dm=37no6.com/003&ac=1510042131161237772&v=y88j6-1.0&rnd=1510042131161237772&ext_y88j6_tid=003&ext_y88j6_uid=1510042131161237772 HTTP/1.1" 200 54 "-" "-" 9CA13069CB4D7B836DC0B8F8FD06F8AF "ImgoTV-iphone/4.5.3.150815 CFNetwork/672.1.13 Darwin/14.0.0" 0.004 test.com.org 1443965482.737

本样例使用的数据，已经通过下述地址开放：`bos://bmr-public-data/logs/accesslog-1k.log`，请大家自行下载使用。


###步骤2：编译、打包并上传程序

具体程序源码见`maven`项目

编译打包后将jar包上传到自己的BOS空间中.

###步骤3：通过BMR Console运行程序

从console页面进去到对应集群的作业列表页面，然后点击“添加作业”，如果使用系统提供的输入数据和jar包，可以按照如下方式填写参数：

>作业类型：Spark

>名称：Scala_PV_UV 

>bos输入地址： bos://${PATH}/bmr-spark-samples-1.0-SNAPSHOT.jar

>失败后操作：继续

>Spark-submit: --class 
com.baidubce.bmr.sample.AccessLogStatsScalaSample

>应用程序参数：bos://bmr-public-data/logs/accesslog-1k.log


Spark driver的输出结果为:

    -----PV-----
    (20151003,139)
    (20151005,372)
    (20151006,114)
    (20151004,375)
    -----UV-----
    (20151003,111)
    (20151005,212)
    (20151006,97)
    (20151004,247)

Spark SQL的用法类似，修改Spark-submit为
`--class com.baidubce.bmr.sample.AccessLogStatsSQLSample`

Spark driver的输出结果为:

    ------PV------
    +--------+---+
    |    date| pv|
    +--------+---+
    |20151003|139|
    |20151004|375|
    |20151005|372|
    |20151006|114|
    +--------+---+

    ------UV------
    +--------+---+
    |    date| uv|
    +--------+---+
    |20151003|111|
    |20151004|247|
    |20151005|212|
    |20151006| 97|
    +--------+---+

可以在bos的文件中查看输出，例如日志url为log，集群id为6c13e8fb-d128-4303-b475-6e92c87df8a2，则driver输出的路径为

`log/6c13e8fb-d128-4303-b475-6e92c87df8a2/task-attempts/application_1446552108370_0002/container_1446552108370_0002_01_000001/stdout`
