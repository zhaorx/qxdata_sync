package job

import (
	"fmt"
	"time"

	"github.com/zhaorx/zlog"
	"qxdata_sync/grpool"
)

const (
	POOL_SIZE  = 10 // 多线程数量
	QUEUE_SIZE = 20 // 任务队列容量
)

var loc, _ = time.LoadLocation("Asia/Shanghai")

type OilHistJob struct {
	BaseJob
}

func NewOilHistJob() *OilHistJob {
	return &OilHistJob{BaseJob: BaseJob{stable: "dba01", typeKey: WELL_KEY_OIL}}
}

// RunHistory 转储单井历史段日数据至taos
func (j OilHistJob) RunHistory() {
	zlog.Info("RunHistory start...")

	start, end, err := parseStartEnd(cfg.HistoryStart, cfg.HistoryEnd)
	if err != nil {
		zlog.Fatalf("parseStartEnd error: " + err.Error())
	}

	list, err := queryWellList(j.typeKey)
	if err != nil {
		zlog.Fatalf("queryWellList error: " + err.Error())
		return
	}

	// 线程池
	pool := grpool.NewPool(POOL_SIZE, QUEUE_SIZE)
	defer pool.Release()
	pool.WaitCount(len(list)) // how many jobs we should wait

	for i := 0; i < len(list); i++ {
		// zlog.Infof("push queue: %d %s\n", i, list[i].WELL_ID)
		x := i
		pool.JobQueue <- func() {
			j.syncWellAll(list[x].WELL_ID, start, end)
			defer pool.JobDone()
		}
	}

	pool.WaitAll()
	zlog.Info("RunHistory end...")
}

// 转储某井所有时间段的数据
func (j OilHistJob) syncWellAll(well_id string, start time.Time, end time.Time) {
	// 匹分时间区间
	count := 0
	for _, r := range getRanges(start, end) {
		// 查询额外数据 动液面 沉没度等
		exdatas, err := j.queryExtraByRange(well_id, r[0], r[1])
		if err != nil {
			zlog.Info(err.Error())
			continue
		}
		// 遍历exdatas 处理成map
		exmap := make(map[int64]OilData)
		for i, d := range exdatas {
			exmap[d.RQ.UnixMicro()] = exdatas[i]
		}

		// 查询主体日度数据
		datas, err := j.queryDataByRange(well_id, r[0], r[1])
		if err != nil {
			zlog.Info(err.Error())
			continue
		}

		// 遍历datas 赋值extra项
		for _, d := range datas {
			ex, ok := exmap[d.RQ.UnixMicro()]
			if ok {
				d.DYNAMIC_LIQ_LEVEL = ex.DYNAMIC_LIQ_LEVEL
				d.CMD = ex.CMD
			}
		}

		if len(datas) > 0 {
			err = insertBatchOilData(datas, j.tableName(datas[0].WELL_ID))
			if err != nil {
				zlog.Info(err.Error())
				continue
			} else {
				count += len(datas)
			}
		}
	}

	zlog.Infof("sync done: %s[%d] \n", well_id, count)
}

// 查询单井阶段数据
func (j OilHistJob) queryDataByRange(well_id string, start time.Time, end time.Time) (list []OilData, err error) {
	sql := fmt.Sprintf("SELECT RQ,WELL_ID,JH,CYFS,SCSJ,BJ,PL,CC,CC1,YY,TY,HY,SXDL,XXDL,RCYL1,RCYL,RCSL,QYHS,"+
		"HS,BZ FROM %s WHERE WELL_ID=:1 AND RQ BETWEEN :2 AND :3",
		j.stable)
	list = make([]OilData, 0, 0)
	err = db.Select(&list, sql, well_id, start, end)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 查询单井额外数据 动液面
func (j OilHistJob) queryExtraByRange(well_id string, start time.Time, end time.Time) (list []OilData, err error) {
	sql := `
		SELECT TEST_DATE AS RQ,DYNAMIC_LIQ_LEVEL,PUMP_DEPTH - DYNAMIC_LIQ_LEVEL AS CMD FROM (
			SELECT
				s.TEST_DATE,
				s.DYNAMIC_LIQ_LEVEL,
				CASE WHEN s.PUMP_DEPTH IS NOT NULL 
				THEN s.PUMP_DEPTH
				ELSE (
					SELECT
						PUMP_DEPTH
					FROM
						TEMP_WELL_MECH_ALL
					WHERE
						TEST_DATE = ( 
							SELECT MAX( TEST_DATE ) FROM TEMP_WELL_MECH_ALL x 
							WHERE x.TEST_DATE <= s.TEST_DATE 
							AND x.WELL_ID = :1
							AND x.PUMP_DEPTH IS NOT NULL 
							AND x.DYNAMIC_LIQ_LEVEL IS NOT NULL 
						) -- 查询每个日期往前最近的有泵深数据的日期
						AND WELL_ID = :2
				) -- 查询每个日期往前最近的泵深数据
				END AS PUMP_DEPTH
			FROM
				TEMP_WELL_MECH_ALL s
			WHERE
				s.WELL_ID = :3
				AND s.DYNAMIC_LIQ_LEVEL IS NOT NULL
				AND s.TEST_DATE BETWEEN :4 AND :5
			ORDER BY s.TEST_DATE
		)
	`
	list = make([]OilData, 0, 0)
	err = db.Select(&list, sql, well_id, well_id, well_id, start, end)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 写taos 拼接多value insert
func insertBatchOilData(list []OilData, table string) error {
	suffix := ""
	for i := 0; i < len(list); i++ {
		item := list[i]
		rqstr := item.RQ.In(loc).Format(time.RFC3339Nano)
		suffix += fmt.Sprintf(` ('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s',%f,%f ) `,
			rqstr, item.CYFS.String,
			item.SCSJ.Float64, item.BJ.Float64, item.PL.Float64, item.CC.Float64,
			item.CC1.Float64, item.YY.Float64, item.TY.Float64, item.HY.Float64,
			item.SXDL.Float64, item.XXDL.Float64, item.RCYL1.Float64, item.RCYL.Float64,
			item.RCSL.Float64, item.QYHS.Float64, item.HS.Float64, item.BZ.String, item.DYNAMIC_LIQ_LEVEL.Float64, item.CMD.Float64)
	}

	insert_sql := `INSERT INTO %s.%s VALUES ` + suffix
	sql := fmt.Sprintf(insert_sql, cfg.TD.DataBase, table)
	_, err := taos.Exec(sql)
	if err != nil {
		// zlog.Info("insert failed: " + sql)
		return err
	}

	return nil
}

type WaterHistJob struct {
	BaseJob
}

func NewWaterHistJob() *WaterHistJob {
	return &WaterHistJob{BaseJob: BaseJob{stable: "dba02", typeKey: WELL_KEY_WATER}}
}

// RunHistory 转储单井历史段日数据至taos
func (j WaterHistJob) RunHistory() {
	zlog.Info("RunHistory start...")

	start, end, err := parseStartEnd(cfg.HistoryStart, cfg.HistoryEnd)
	if err != nil {
		zlog.Fatalf("parseStartEnd error: " + err.Error())
	}

	list, err := queryWellList(j.typeKey)
	if err != nil {
		zlog.Fatalf("queryWellList error: " + err.Error())
		return
	}

	// 线程池
	pool := grpool.NewPool(POOL_SIZE, QUEUE_SIZE)
	defer pool.Release()
	pool.WaitCount(len(list)) // how many jobs we should wait

	for i := 0; i < len(list); i++ {
		// zlog.Infof("push queue: %d %s\n", i, list[i].WELL_ID)
		x := i
		pool.JobQueue <- func() {
			j.syncWellAll(list[x].WELL_ID, start, end)
			defer pool.JobDone()
		}
	}

	pool.WaitAll()
	zlog.Info("RunHistory end...")
}

// 转储某井所有时间段的数据
func (j WaterHistJob) syncWellAll(well_id string, start time.Time, end time.Time) {
	// 匹分时间区间
	count := 0
	for _, r := range getRanges(start, end) {
		datas, err := j.queryDataByRange(well_id, r[0], r[1])
		if err != nil {
			zlog.Info(err.Error())
			continue
		}

		if len(datas) > 0 {
			err = insertBatchWaterData(datas, j.tableName(datas[0].WELL_ID))
			if err != nil {
				zlog.Info(err.Error())
				continue
			} else {
				count += len(datas)
			}
		}
	}

	zlog.Infof("sync done: %s[%d] \n", well_id, count)
}

// 查询单井阶段数据
func (j WaterHistJob) queryDataByRange(well_id string, start time.Time, end time.Time) (list []WaterData, err error) {
	sql := fmt.Sprintf("SELECT RQ,WELL_ID,JH,SCSJ,ZSFS,PZCDS,RPZSL,RZSL,GXYL,TY,YY,BZ FROM %s WHERE WELL_ID=:1 AND RQ BETWEEN :2 AND :3", j.stable)
	list = make([]WaterData, 0, 0)
	err = db.Select(&list, sql, well_id, start, end)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 写taos 拼接多value insert
func insertBatchWaterData(list []WaterData, table string) error {
	suffix := ""
	for i := 0; i < len(list); i++ {
		item := list[i]
		rqstr := item.RQ.In(loc).Format(time.RFC3339Nano)
		suffix += fmt.Sprintf(` ('%s',%f,'%s',%f,%f,%f,%f,%f,%f,'%s') `,
			rqstr, item.SCSJ.Float64,
			item.ZSFS.String, item.PZCDS.Float64, item.RPZSL.Float64, item.RZSL.Float64,
			item.GXYL.Float64, item.YY.Float64, item.TY.Float64, item.BZ.String)
	}

	insert_sql := `INSERT INTO %s.%s VALUES ` + suffix
	sql := fmt.Sprintf(insert_sql, cfg.TD.DataBase, table)
	_, err := taos.Exec(sql)
	if err != nil {
		// zlog.Info("insert failed: " + sql)
		return err
	}

	return nil
}
