这是一个电商分析项目

目前有三个模块:✨

    1🎉.common:存放的是一些公共工具,比如配置文件,JDBCUtil,读取配置文件的Util等.

    2🎉.mock:由于这是自己单机实现的项目,没有数据源,所以自己代码生成一些数据,以供后面分析.

    3🎉.offline:离线数据分析模块,目前有六个功能,根据sparkSession来进行RDD运算.

2018-12-12:更新

    🌹增加MockerRealtime类:用于生产实时处理需要的数据;

    🌹增加MyRealtiUtil类:建立redis连接池;

    🌹增加realtime模块:用于实时处理数据.
