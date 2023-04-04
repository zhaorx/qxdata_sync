package job

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"qxdata_sync/config"
	"qxdata_sync/database"
	"qxdata_sync/util"
)

var cfg = config.Cfg
var logger *log.Logger
var db *sqlx.DB
var taos *sqlx.DB
var size int64 = 100 // 批量insert的天数

// 初始化目标数据库连接
func init() {
	// init db
	var err error
	db, err = database.ConnectDB(cfg.DB)
	if err != nil {
		log.Fatalln("db init error: " + err.Error())
	}

	// init taos
	taos, err = database.ConnectTaos(cfg.TD)
	if err != nil {
		log.Fatalln("taos init error: " + err.Error())
	}
}

// RunDaily 每日转储单井日数据至taos
func RunDaily() {

}

// RunHistory 转储单井历史段日数据至taos
func RunHistory() {
	logger = util.InitLog("history")

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

	for _, well := range list {
		syncWellAll(well.WELL_ID, start, end)
	}

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

func syncWellAll(well_id string, start time.Time, end time.Time) {
	// 匹分时间区间
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
			}
		}
	}
}

func insertBatchData(list []Data) error {
	// 写taos 拼接多value insert
	suffix := ""
	for i := 0; i < len(list); i++ {
		item := list[i]
		rqstr := item.RQ.Format("2006-01-02 15:04:05")
		suffix += fmt.Sprintf(` ('%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s') `,
			rqstr, item.JH.String, item.WELL_ID, item.CYFS.String,
			item.SCSJ.Float64, item.BJ.Float64, item.PL.Float64, item.CC.Float64,
			item.CC1.Float64, item.YY.Float64, item.TY.Float64, item.HY.Float64,
			item.SXDL.Float64, item.XXDL.Float64, item.RCYL1.Float64, item.RCYL.Float64,
			item.RCSL.Float64, item.QYHS.Float64, item.HS.Float64, item.BZ.String)
	}

	insert_sql := `INSERT INTO %s.%s VALUES ` + suffix
	sql := fmt.Sprintf(insert_sql, cfg.TD.DataBase, list[0].WELL_ID)
	_, err := taos.Exec(sql)
	if err != nil {
		logger.Println("insert failed: " + sql)
		return err
	}

	return nil
}

// 查询单井日数据
func queryOneDayData(rq time.Time) (list []Data, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ=:1 AND JH='15W2-20-25'", cfg.DB.DataTable) // 只处理15W2-20-25
	list = make([]Data, 0, 0)
	err = db.Select(&list, sql, rq)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

// 查询单井阶段数据
func queryDataByRange(well_id string, start time.Time, end time.Time) (list []Data, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE WELL_ID=:1 AND RQ BETWEEN :2 AND :3", cfg.DB.DataTable) // 只处理15W2-20-25
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

	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	sql := fmt.Sprintf("SELECT WELL_ID,WELL_DESC AS JH FROM \"%s\"", cfg.DB.WellTable) // 只处理15W2-20-25
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

type Data struct {
	RQ       time.Time       `db:"RQ"`
	WELL_ID  string          `db:"WELL_ID"`
	JH       sql.NullString  `db:"JH"`
	CYFS     sql.NullString  `db:"CYFS"`
	BZDM1    sql.NullString  `db:"BZDM1"`
	BZDM2    sql.NullString  `db:"BZDM2"`
	HS       sql.NullFloat64 `db:"HS"`
	HS1      sql.NullFloat64 `db:"HS1"`
	JCDM     sql.NullString  `db:"JCDM"`
	QYB      sql.NullFloat64 `db:"QYB"`
	RCQL     sql.NullFloat64 `db:"RCQL"`
	RCSL     sql.NullFloat64 `db:"RCSL"`
	RCYL     sql.NullFloat64 `db:"RCYL"`
	RCYL1    sql.NullFloat64 `db:"RCYL1"`
	RXBZ     sql.NullString  `db:"RXBZ"`
	SCSJ     sql.NullFloat64 `db:"SCSJ"`
	CSWD     sql.NullFloat64 `db:"CSWD"`
	CSYL     sql.NullFloat64 `db:"CSYL"`
	DBDLC    sql.NullFloat64 `db:"DBDLC"`
	DBDY     sql.NullFloat64 `db:"DBDY"`
	HY       sql.NullFloat64 `db:"HY"`
	HYWD     sql.NullFloat64 `db:"HYWD"`
	RCYHS    sql.NullFloat64 `db:"RCYHS"`
	RCYL2    sql.NullFloat64 `db:"RCYL2"`
	SXDL     sql.NullFloat64 `db:"SXDL"`
	XXDL     sql.NullFloat64 `db:"XXDL"`
	TY       sql.NullFloat64 `db:"TY"`
	YY       sql.NullFloat64 `db:"YY"`
	YZ       sql.NullFloat64 `db:"YZ"`
	JKWD     sql.NullFloat64 `db:"JKWD"`
	QYHS     sql.NullFloat64 `db:"QYHS"`
	XYLYRCYL sql.NullFloat64 `db:"XYLYRCYL"`
	CC       sql.NullFloat64 `db:"CC"`
	CC1      sql.NullFloat64 `db:"CC1"`
	PL       sql.NullFloat64 `db:"PL"`
	BJ       sql.NullFloat64 `db:"BJ"`
	BX       sql.NullFloat64 `db:"BX"`
	CCBHJND  sql.NullFloat64 `db:"CCBHJND"`
	CCJND    sql.NullFloat64 `db:"CCJND"`
	HYJHWND  sql.NullFloat64 `db:"HYJHWND"`
	BZ       sql.NullString  `db:"BZ"`
}

type Well struct {
	WELL_ID string         `db:"WELL_ID"`
	JH      sql.NullString `db:"JH"`
	CYC     sql.NullString `db:"CANTON"`
	GLQ     sql.NullString `db:"CYKMC"`
	CYD     sql.NullString `db:"CYDMC"`
	QK      sql.NullString `db:"PROJECT_NAME"`
}
