package job

import (
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/errors"
	"qxdata_sync/util"
)

var tableNamePrefix = "TD_"

// RunScheama 同步单井表和表tag
func RunScheama() {
	logger = util.InitLog("scheama")
	// 查询所有单井基本信息
	list, err := queryWellList()
	if err != nil {
		logger.Fatalf("queryWellList error: " + err.Error())
		return
	}

	logger.Printf("start sync tables scheama, count: %v\n", len(list))

	for i := 0; i < len(list); i++ {
		wellId := list[i].WELL_ID
		// 表存在 update tag 表不存在 create table
		if isTableExist(wellId) {
			updateWellTableTag(list[i])
		} else {
			createWellTable(list[i])
		}
	}

	logger.Printf("sync tables scheama end......\n")
}

func createWellTable(w Well) {
	sqlStr := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING %s.%s TAGS ('%s','%s','%s','%s','%s','%s','%s');`, tableNamePrefix+w.WELL_ID,
		cfg.TD.DataBase, cfg.TD.STable, w.CYC.String, w.GLQ.String, w.CYD.String, w.CYB.String, w.QK.String, w.JH.String, w.WELL_ID)
	_, err := taos.Exec(sqlStr)
	if err != nil {
		logger.Fatalln("failed to create table:", err)
	}

	logger.Printf("create table %s\n", tableNamePrefix+w.WELL_ID)
}

func updateWellTableTag(w Well) {
	logger.Printf("start update table %s tag\n", tableNamePrefix+w.WELL_ID)
	m := map[string]string{
		"CYC":     w.CYC.String,
		"GLQ":     w.GLQ.String,
		"CYD":     w.CYD.String,
		"CYB":     w.CYB.String,
		"QK":      w.QK.String,
		"JH":      w.JH.String,
		"WELL_ID": w.WELL_ID,
	}

	for k, v := range m {
		sqlStr := fmt.Sprintf(`ALTER TABLE %s SET TAG %s='%s'`, tableNamePrefix+w.WELL_ID, k, v)
		time.Sleep(time.Millisecond * 100)
		_, err := taos.Exec(sqlStr)
		if err != nil {
			logger.Fatalln("failed to update table tag:", err)
		}
	}

	logger.Printf("update table %s tag\n", tableNamePrefix+w.WELL_ID)
}

// 判断井table是否存在 使用wellId作为表名
func isTableExist(wellId string) bool {
	// sqlStr := fmt.Sprintf(`SHOW TABLES LIKE '%s'`, wellId)
	sqlStr := fmt.Sprintf(`select last_row(*) from %s`, tableNamePrefix+wellId)
	rows, err := taos.Query(sqlStr)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		if e, ok := err.(*errors.TaosError); ok && e.Code == 9826 {
			return false
		}
	}

	return true
}
