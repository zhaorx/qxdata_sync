package job

import "time"

type ScheamaJob interface {
	RunScheama()
	isTableExist(w Well) bool
	createTable(w Well)
	updateTag(w Well)
	tableName(well_id string) string
}

type HistoryJob interface {
	RunHistory()
	syncWellAll(well_id string, start time.Time, end time.Time)
}
