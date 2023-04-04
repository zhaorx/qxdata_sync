package job

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"gopkg.in/natefinch/lumberjack.v2"
	"qxdata_sync/config"
	"qxdata_sync/database"
)

var cfg = config.Cfg
var logger *log.Logger
var db *sqlx.DB
var taos *sqlx.DB

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
	initLog("history")

	if len(cfg.HistoryStart) == 0 || len(cfg.HistoryEnd) == 0 {
		logger.Fatalf("请正确设置历史数据抓取时间段historyStart和historyEnd")
	}

	loc, _ := time.LoadLocation("Local")
	startTiming, err := time.ParseInLocation("2006-01-02", cfg.HistoryStart, loc)
	if err != nil {
		logger.Fatalf("时间解析错误,请使用2006-01-02格式")
	}
	endTiming, err := time.ParseInLocation("2006-01-02", cfg.HistoryEnd, loc)
	if err != nil {
		logger.Fatalf("时间解析错误,请使用2006-01-02格式")
	}

	for t := startTiming; t.Before(endTiming.Add(time.Minute)); t = t.Add(time.Hour * 24) {
		list, err := queryOneDayData(t)
		if err != nil {
			logger.Println(err.Error())
			continue
		}
		// fmt.Println(len(list))

		// 写taos 拼接多value insert
		// suffix := ""
		for i := 0; i < len(list); i++ {
			item := list[i]
			rqstr := item.RQ.Format("2006-01-02 15:04:05")
			// suffix += fmt.Sprintf(` ('%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f) `, rqstr, item.JH, item.WELL_ID, item.CYFS,
			// 	item.SCSJ, item.BJ, &item.PL, item.CC, item.CC1, item.YY, item.TY, item.HY, item.SXDL, item.XXDL, item.RCYL1, item.RCYL, item.RCSL,
			// 	item.QYHS, item.HS, item.BZ)

			insert_sql := `INSERT INTO %s.%s VALUES `
			sql := fmt.Sprintf(insert_sql, cfg.TD.DataBase, list[0].WELL_ID) + fmt.Sprintf(` ('%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,
%f,%f,%f,%f,'%s') `, rqstr, item.JH.String, item.WELL_ID, item.CYFS.String,
				item.SCSJ.Float64, item.BJ.Float64, item.PL.Float64, item.CC.Float64,
				item.CC1.Float64, item.YY.Float64, item.TY.Float64, item.HY.Float64,
				item.SXDL.Float64, item.XXDL.Float64, item.RCYL1.Float64, item.RCYL.Float64,
				item.RCSL.Float64, item.QYHS.Float64, item.HS.Float64, item.BZ.String)
			_, err := taos.Exec(sql)
			if err != nil {
				logger.Println(err.Error())
				logger.Println("insert failed: " + sql)
				continue
			}
		}

		time.Sleep(time.Second * 1)
	}
}

func initLog(prefix string) {
	// 1. init log
	if cfg.Profile == "prod" {
		logger = log.New(&lumberjack.Logger{
			Filename:   prefix + ".log",
			MaxSize:    2, // megabytes
			MaxBackups: 3,
			MaxAge:     30, // days
		}, prefix, log.Lshortfile|log.Ldate|log.Ltime)
	} else {
		logger = log.New(os.Stdout, prefix, log.Lshortfile|log.Ldate|log.Ltime)
	}
}

// 查询单日期数据
func queryOneDayData(rq time.Time) (list []Data, err error) {
	if len(cfg.DB.DataTable) == 0 {
		return list, errors.New("cfg.DB.DataTable is null")
	}

	// sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1", cfg.DB.DataTable)
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE RQ =:1 AND JH='15W2-20-25'", cfg.DB.DataTable) // 只处理15W2-20-25
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
