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
