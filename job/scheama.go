package job

import (
	"fmt"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/zhaorx/zlog"
)

type OilSchJob struct {
	BaseJob
}

func NewOilSchJob() *OilSchJob {
	return &OilSchJob{BaseJob: BaseJob{stable: "dba01", typeKey: WELL_KEY_OIL}}
}

type WaterSchJob struct {
	BaseJob
}

func NewWaterSchJob() *OilSchJob {
	return &OilSchJob{BaseJob: BaseJob{stable: "dba02", typeKey: WELL_KEY_WATER}}
}

type BaseJob struct {
	stable  string
	typeKey string
}

func (j BaseJob) RunScheama() {
	// zlog = util.InitLog("scheama_" + j.stable + " ")
	// 查询所有单井基本信息
	list, err := queryWellList(j.typeKey)
	if err != nil {
		zlog.Fatalf("queryWellList error: " + err.Error())
		return
	}

	zlog.Infof("start sync tables scheama, count: %v\n", len(list))
	for i := 0; i < len(list); i++ {
		// 表存在 update tag 表不存在 create table
		if j.isTableExist(list[i]) {
			j.updateTag(list[i])
		} else {
			j.createTable(list[i])
		}
	}

	zlog.Infof("sync tables scheama end......\n")
}

func (j BaseJob) isTableExist(w Well) bool {
	// sqlStr := fmt.Sprintf(`SHOW TABLES LIKE '%s'`, wellId)
	sqlStr := fmt.Sprintf(`select last_row(*) from %s`, j.tableName(w.WELL_ID))
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

func (j BaseJob) createTable(w Well) {
	tname := j.tableName(w.WELL_ID)
	sqlStr := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING %s.%s TAGS ('%s','%s','%s','%s','%s','%s','%s');`, tname,
		cfg.TD.DataBase, j.stable, w.CYC.String, w.GLQ.String, w.CYD.String, w.CYB.String, w.QK.String, w.JH.String, w.WELL_ID)
	_, err := taos.Exec(sqlStr)
	if err != nil {
		zlog.Fatalf("failed to create table:", err)
	}

	zlog.Infof("create table %s\n", tname)
}

func (j BaseJob) updateTag(w Well) {
	tname := j.tableName(w.WELL_ID)
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
		sqlStr := fmt.Sprintf(`ALTER TABLE %s SET TAG %s='%s'`, tname, k, v)
		_, err := taos.Exec(sqlStr)
		if err != nil {
			zlog.Error("failed to update table tag:" + err.Error())
		}
	}

	zlog.Infof("update table %s tag\n", tname)
}

func (j BaseJob) tableName(well_id string) string {
	return j.stable + "_" + well_id
}
