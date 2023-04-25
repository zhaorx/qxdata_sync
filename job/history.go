package job

import (
	"errors"
	"fmt"
	"time"

	"qxdata_sync/grpool"
	"qxdata_sync/util"
)

const (
	poolSize  = 10 // 多线程数量
	queueSize = 20 // 任务队列容量
)

var loc, _ = time.LoadLocation("Asia/Shanghai")

// RunHistory 转储单井历史段日数据至taos
func RunHistory() {
	logger = util.InitLog("history")
	logger.Println("RunHistory start...")

	if len(cfg.HistoryStart) == 0 || len(cfg.HistoryEnd) == 0 {
		logger.Fatalf("请正确设置历史数据抓取时间段historyStart和historyEnd")
	}

	loc, _ := time.LoadLocation("Local")
	start, err := time.ParseInLocation("2006-01-02", cfg.HistoryStart, loc)
	if err != nil {
		logger.Fatalf("时间解析错误,请使用2006-01-02格式")
	}
	end, err := time.ParseInLocation("2006-01-02", cfg.HistoryEnd, loc)
	if err != nil {
		logger.Fatalf("时间解析错误,请使用2006-01-02格式")
	}

	list, err := queryWellList()
	if err != nil {
		logger.Fatalf("queryWellList error: " + err.Error())
		return
	}

	// 线程池
	pool := grpool.NewPool(poolSize, queueSize)
	defer pool.Release()
	pool.WaitCount(len(list)) // how many jobs we should wait

	for i := 0; i < len(list); i++ {
		// logger.Printf("push queue: %d %s\n", i, list[i].WELL_ID)
		x := i
		pool.JobQueue <- func() {
			syncWellAll(list[x].WELL_ID, start, end)
			defer pool.JobDone()
		}
	}

	pool.WaitAll()
	logger.Println("RunHistory end...")
}

// 转储某井所有时间段的数据
func syncWellAll(well_id string, start time.Time, end time.Time) {
	// logger.Printf("sync start: " + well_id)
	// 匹分时间区间
	count := 0
	for _, r := range getRanges(start, end) {
		datas, err := queryDataByRange(well_id, r[0], r[1])
		if err != nil {
			logger.Println(err.Error())
			continue
		}

		if len(datas) > 0 {
			err = insertBatchData(datas)
			if err != nil {
				logger.Println(err.Error())
				continue
			} else {
				count += len(datas)
			}
		}
	}

	logger.Printf("sync done: %s[%d] \n", well_id, count)
}

func insertBatchData(list []Data) error {
	// 写taos 拼接多value insert
	suffix := ""

	for i := 0; i < len(list); i++ {
		item := list[i]
		rqstr := item.RQ.In(loc).Format(time.RFC3339Nano)
		suffix += fmt.Sprintf(` ('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s') `,
			rqstr, item.CYFS.String,
			item.SCSJ.Float64, item.BJ.Float64, item.PL.Float64, item.CC.Float64,
			item.CC1.Float64, item.YY.Float64, item.TY.Float64, item.HY.Float64,
			item.SXDL.Float64, item.XXDL.Float64, item.RCYL1.Float64, item.RCYL.Float64,
			item.RCSL.Float64, item.QYHS.Float64, item.HS.Float64, item.BZ.String)
	}

	insert_sql := `INSERT INTO %s.%s VALUES ` + suffix
	sql := fmt.Sprintf(insert_sql, cfg.TD.DataBase, tableNamePrefix+list[0].WELL_ID)
	_, err := taos.Exec(sql)
	if err != nil {
		// logger.Println("insert failed: " + sql)
		return err
	}

	return nil
}

// 查询单井阶段数据
func queryDataByRange(well_id string, start time.Time, end time.Time) (list []Data, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE WELL_ID=:1 AND RQ BETWEEN :2 AND :3", cfg.DB.DataTable)
	list = make([]Data, 0, 0)
	err = db.Select(&list, sql, well_id, start, end)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 查询单井列表
func queryWellList() (list []Well, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	sql := fmt.Sprintf("SELECT WELL_ID,WELL_DESC,CANTON,CYKMC,CYDMC,PROJECT_NAME FROM \"%s\"", cfg.DB.WellTable)
	list = make([]Well, 0, 0)
	err = db.Select(&list, sql)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 匹分时间区间
func getRanges(start time.Time, end time.Time) [][]time.Time {
	ranges := make([][]time.Time, 0)
	sizeDur := time.Duration(size) * time.Hour * 24
	cur := start
	for ; cur.Before(end); cur = cur.Add(sizeDur) {
		item := make([]time.Time, 2)
		item[0] = cur
		item[1] = cur.Add(sizeDur).Add(time.Hour * -24)
		if cur.Add(sizeDur).Add(time.Hour * -24).After(end) {
			item[1] = end
		}

		ranges = append(ranges, item)
	}

	return ranges
}
