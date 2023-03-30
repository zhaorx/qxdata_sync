package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

var Cfg Config

const configPath = "./config.yml"

// 加载log
func init() {
	// load cfg
	data, err := os.ReadFile(configPath)
	if err != nil {
		panic(err.Error())
	}

	Cfg = NewConfigWithDefault()
	err = yaml.Unmarshal(data, &Cfg)
	if err != nil {
		panic(err)
	}
}

// cfg 缺省设置
func NewConfigWithDefault() Config {
	c := Config{
		Profile: "dev",
		Cron:    "0 0 1 1 * ?",
	}
	return c
}

type Config struct {
	Profile string `yaml:"profile"` // 执行环境 dev/prod/history/org
	Cron    string `yaml:"cron"`
	DB      DB     `yaml:"db"` // oracle
	TD      TD     `yaml:"td"` // taos
	// 拉取历史数据配置
	HistoryStart string `yaml:"historyStart"` // 历史数据抓取-开始日期
	HistoryEnd   string `yaml:"historyEnd"`   // 历史数据抓取-结束日期 缺省默认当前日期

}

// oracle数据库配置
type DB struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	MaxOpenConns int    `yaml:"maxOpenConns"`
	ServiceName  string `yaml:"serviceName"`
	DataTable    string `yaml:"dataTable"`
}

type TD struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	MaxOpenConns int    `yaml:"maxOpenConns"`
	DataBase     string `yaml:"dataBase"`
	STable       string `yaml:"sTable"`
}
