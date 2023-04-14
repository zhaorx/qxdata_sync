package database

import (
	"fmt"
	"log"

	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	// _ "github.com/taosdata/driver-go/v3/taosRestful"
	_ "github.com/taosdata/driver-go/v3/taosSql"
	"qxdata_sync/config"
)

func ConnectDB(cfg config.DB) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`, cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.ServiceName)
	var err error
	DB, err := sqlx.Open("godror", dsn)
	if err != nil {
		return nil, err
	}
	// defer DB.Close()
	err = DB.Ping()
	if err != nil {
		return nil, err
	}

	DB.SetMaxOpenConns(cfg.MaxOpenConns)
	log.Println("db init success：" + cfg.Host)
	// DB.SetMaxIdleConns(0)
	// DB.SetConnMaxLifetime(30 * time.Second)

	return DB, nil
}

// ConnectTaos taos原生连接
func ConnectTaos(cfg config.TD) (*sqlx.DB, error) {
	var taosUri = fmt.Sprintf("%s:%s@tcp(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	taos, err := sqlx.Open("taosSql", taosUri)
	if err != nil {
		log.Fatal("taos init error:%v", err)
	}

	// var taosDSN = fmt.Sprintf("%s:%s@http(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	// taos, err := sqlx.Open("taosRestful", taosDSN)
	// if err != nil {
	// 	log.Fatal("taos init error:%v", err)
	// 	return nil, err
	// }

	taos.SetMaxOpenConns(cfg.MaxOpenConns)
	log.Println("taos init success：" + cfg.Host)
	return taos, nil
}

func ConnectTaosRest(cfg config.TD) (*sqlx.DB, error) {
	// var taosUri = fmt.Sprintf("%s:%s@tcp(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	// fmt.Println("taosUri:", taosUri)
	// taos, err := sql.Open("taosSql", taosUri)
	// if err != nil {
	//	log.Fatal("taos init error:%v", err)
	//	return nil
	// }

	var taosDSN = fmt.Sprintf("%s:%s@http(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	taos, err := sqlx.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatal("taos init error:%v", err)
		return nil, err
	}

	taos.SetMaxOpenConns(cfg.MaxOpenConns)
	log.Println("taos init success：" + cfg.Host)
	return taos, nil
}
