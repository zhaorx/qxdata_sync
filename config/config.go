package config

import (
	"flag"
	"log"
	"os"

	"github.com/zhaorx/zlog"
	"gopkg.in/yaml.v3"
)

var Cfg Config

const defaultConfigPath = "./config.yml"

// 加载log 启动程序使用-c定义配置文件路径 例如: -c ./temp/config.yml
func init() {
	configPath := flag.String("c", defaultConfigPath, "config.yml file path")
	flag.Parse()
	log.Println("config file path -> ", *configPath)

	// load cfg
	data, err := os.ReadFile(*configPath)
	if err != nil {
		panic(err.Error())
	}

	Cfg = NewConfigWithDefault()
	err = yaml.Unmarshal(data, &Cfg)
	if err != nil {
		panic(err)
	}

	Cfg.Log.Profile = Cfg.Profile
	zlog.Init(Cfg.Log)
}

// cfg 缺省设置
func NewConfigWithDefault() Config {
	c := Config{
		Profile: "dev",
		Mode:    "daily",
		Cron:    "0 0 1 1 * ?",
	}
	c.Log = zlog.Conf{
		Profile: c.Profile,
		Path:    "./logs/log",
	}
	return c
}

type Config struct {
	Profile string `yaml:"profile"` // dev/prod
	Mode    string `yaml:"mode"`    // daily/history
	Cron    string `yaml:"cron"`
	DB      DB     `yaml:"db"` // oracle
	TD      TD     `yaml:"td"` // taos
	// 拉取历史数据配置
	HistoryStart string    `yaml:"historyStart"` // 历史数据抓取-开始日期
	HistoryEnd   string    `yaml:"historyEnd"`   // 历史数据抓取-结束日期 缺省默认当前日期
	Log          zlog.Conf `yaml:"log"`
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
	WellTable    string `yaml:"wellTable"`
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
