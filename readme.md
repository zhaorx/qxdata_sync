
##### history sync
-1. queryV_CD_WELL_SOURCE中所有井wells
-2. 遍历wells 每次遍历 将总时间段按照100天分成N个区间 ranges
-3. 遍历ranges 拼接成一个100values的insert sql 写入taos
-4. 按照这个遍历逻辑继续 直到所有jh的所有range都insert taos
-5. 多线程改造
-6. 替换原生连接taos
7. 参数绑定
8. 完善log
-9. flag 自定义config文件位置 根据模式切换daily history

##### tag sync
-1. 每日同步 V_CD_WELL_SOURCE 中的单井基本信息wells
-2. 遍历wells 将每口井的cyc cyd等tag insert/update 至taos 保持每口井的tag数据与oracle一致
3. 多线程改造

##### daily sync
1. 查询当前时间点前一天的所有井的日数据list
2. 遍历list 拼接成insert sql 写入taos
