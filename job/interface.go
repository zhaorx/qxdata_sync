package job

import "time"

type ScheamaJob interface {
	runScheama()
	isTableExist(w Well) bool
	createTable(w Well)
	updateTag(w Well)
	tableName(well_id string) string
}

type HistoryJob interface {
	runHistory(func(string, time.Time, time.Time))
}
