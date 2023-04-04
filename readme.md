1. query时间段范围内共有多少口井的数据jhs
2. 遍历jhs 时间段按照100天分成N个区间 ranges
3. 遍历ranges 拼接成一个100values的insert sql 写入taos
4. 按照这个遍历逻辑继续 直到所有jh的所有range都insert taos

