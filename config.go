package redisx

type Config struct {
	// 通信地址
	Address string `default:"127.0.0.1:6379" yaml:"address" validate:"required"`
	// 用户名
	Username string `yaml:"username"`
	// 授权密码
	Password string `yaml:"password"`
	// 数据库
	DB int `yaml:"db" validate:"min=0"`
	// 最大的socket连接数
	PoolSize int `default:"10" yaml:"poolSize" validate:"min=0"`
	// 键前缀
	Prefix string `yaml:"prefix"`
}
