package job

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zhaorx/zlog"
	"qxdata_sync/config"
	"qxdata_sync/database"
)

const (
	WELL_KEY_OIL   = "1"
	WELL_KEY_WATER = "3"
)

var cfg = config.Cfg
var db *sqlx.DB
var taos *sqlx.DB
var size int64 = 100 // 批量insert的天数

// 初始化目标数据库连接
func init() {
	// init db
	var err error
	db, err = database.ConnectDB(cfg.DB)
	if err != nil {
		zlog.Fatal("db init error: " + err.Error())
	}

	// init taos
	taos, err = database.ConnectTaos(cfg.TD)
	if err != nil {
		zlog.Fatal("taos init error: " + err.Error())
	}
}

// RunDaily 每日转储单井日数据至taos
func RunDaily() {

}

// 查询单井基本信息列表
func queryWellList(well_key string) (list []Well, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	sql := fmt.Sprintf("SELECT WELL_ID,WELL_DESC,CANTON,CYKMC,CYDMC,PROJECT_NAME FROM \"%s\" WHERE SUBSTR(WELL_PURPOSE,0,1)='%s'", cfg.DB.WellTable, well_key)
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

func parseStartEnd(startStr, endStr string) (start, end time.Time, err error) {
	if len(startStr) == 0 || len(cfg.HistoryEnd) == 0 {
		zlog.Fatalf("请正确设置历史数据抓取时间段historyStart和historyEnd")
	}

	loc, _ := time.LoadLocation("Local")
	start, err = time.ParseInLocation("2006-01-02", startStr, loc)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("时间解析错误,请使用2006-01-02格式")
	}
	end, err = time.ParseInLocation("2006-01-02", endStr, loc)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("时间解析错误,请使用2006-01-02格式")
	}

	return start, end, nil
}

// 查询单井日数据
func queryOneDayData(rq time.Time) (list []OilData, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ=:1 AND JH='15W2-20-25'", cfg.DB.DataTable) // 只处理15W2-20-25
	list = make([]OilData, 0, 0)
	err = db.Select(&list, sql, rq)
	if err != nil {
		return list, err
	}

	if err != nil {
		return list, err
	}

	return list, nil
}

type OilData struct {
	RQ                time.Time       `db:"RQ"`
	WELL_ID           string          `db:"WELL_ID"`
	JH                sql.NullString  `db:"JH"`
	CYFS              sql.NullString  `db:"CYFS"`
	BZDM1             sql.NullString  `db:"BZDM1"`
	BZDM2             sql.NullString  `db:"BZDM2"`
	HS                sql.NullFloat64 `db:"HS"`
	HS1               sql.NullFloat64 `db:"HS1"`
	JCDM              sql.NullString  `db:"JCDM"`
	QYB               sql.NullFloat64 `db:"QYB"`
	RCQL              sql.NullFloat64 `db:"RCQL"`
	RCSL              sql.NullFloat64 `db:"RCSL"`
	RCYL              sql.NullFloat64 `db:"RCYL"`
	RCYL1             sql.NullFloat64 `db:"RCYL1"`
	RXBZ              sql.NullString  `db:"RXBZ"`
	SCSJ              sql.NullFloat64 `db:"SCSJ"`
	CSWD              sql.NullFloat64 `db:"CSWD"`
	CSYL              sql.NullFloat64 `db:"CSYL"`
	DBDLC             sql.NullFloat64 `db:"DBDLC"`
	DBDY              sql.NullFloat64 `db:"DBDY"`
	HY                sql.NullFloat64 `db:"HY"`
	HYWD              sql.NullFloat64 `db:"HYWD"`
	RCYHS             sql.NullFloat64 `db:"RCYHS"`
	RCYL2             sql.NullFloat64 `db:"RCYL2"`
	SXDL              sql.NullFloat64 `db:"SXDL"`
	XXDL              sql.NullFloat64 `db:"XXDL"`
	TY                sql.NullFloat64 `db:"TY"`
	YY                sql.NullFloat64 `db:"YY"`
	YZ                sql.NullFloat64 `db:"YZ"`
	JKWD              sql.NullFloat64 `db:"JKWD"`
	QYHS              sql.NullFloat64 `db:"QYHS"`
	XYLYRCYL          sql.NullFloat64 `db:"XYLYRCYL"`
	CC                sql.NullFloat64 `db:"CC"`
	CC1               sql.NullFloat64 `db:"CC1"`
	PL                sql.NullFloat64 `db:"PL"`
	BJ                sql.NullFloat64 `db:"BJ"`
	BX                sql.NullFloat64 `db:"BX"`
	CCBHJND           sql.NullFloat64 `db:"CCBHJND"`
	CCJND             sql.NullFloat64 `db:"CCJND"`
	HYJHWND           sql.NullFloat64 `db:"HYJHWND"`
	BZ                sql.NullString  `db:"BZ"`
	DESCRIPTION       sql.NullString  `db:"DESCRIPTION"`
	DYNAMIC_LIQ_LEVEL sql.NullFloat64 `db:"DYNAMIC_LIQ_LEVEL"` // 动液面
	CMD               sql.NullFloat64 `db:"CMD"`               // 沉没度
}

type WaterData struct {
	RQ      time.Time       `db:"RQ"`
	WELL_ID string          `db:"WELL_ID"`
	JH      sql.NullString  `db:"JH"`
	SCSJ    sql.NullFloat64 `db:"SCSJ"`
	ZSFS    sql.NullString  `db:"ZSFS"`
	PZCDS   sql.NullFloat64 `db:"PZCDS"`
	RPZSL   sql.NullFloat64 `db:"RPZSL"`
	RZSL    sql.NullFloat64 `db:"RZSL"`
	GXYL    sql.NullFloat64 `db:"GXYL"`
	TY      sql.NullFloat64 `db:"TY"`
	YY      sql.NullFloat64 `db:"YY"`
	BZ      sql.NullString  `db:"BZ"`
}

type Well struct {
	WELL_ID string         `db:"WELL_ID"`
	JH      sql.NullString `db:"WELL_DESC"`
	CYC     sql.NullString `db:"CANTON"`
	GLQ     sql.NullString `db:"CYKMC"`
	CYD     sql.NullString `db:"CYDMC"`
	CYB     sql.NullString `db:"CYB"`
	QK      sql.NullString `db:"PROJECT_NAME"`
	// CYCNAME sql.NullString `db:"CANTON"`
	// GLQNAME sql.NullString `db:"CYKMC"`
	// CYDNAME sql.NullString `db:"CYDMC"`
	// CYBNAME sql.NullString `db:"CYBMC"`
	// QKNAME  sql.NullString `db:"PROJECT_NAME"`
}
