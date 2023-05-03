package job

import (
	"errors"
	"fmt"
	"time"

	"github.com/zhaorx/zlog"
)

const (
	WELL_KEY_OIL   = "1"
	WELL_KEY_WATER = "3"
)

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
