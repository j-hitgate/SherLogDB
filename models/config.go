package models

import "time"

type Config struct {
	Port      string
	Writers   byte
	Readers   byte
	Deleters  byte
	Password  string
	LogLevel  byte
	LogsDir   string
	Scheduler SchedulerConfig
}

func (c *Config) EmptyToDefault() {
	if c.Port == "" {
		c.Port = "8070"
	}

	if c.Writers == 0 {
		c.Writers = 10
	}

	if c.Readers == 0 {
		c.Readers = 10
	}

	if c.Deleters == 0 {
		c.Deleters = 1
	}

	c.Scheduler.EmptyToDefault()
}

type SchedulerConfig struct {
	LogsTTL          time.Duration
	AligningPeriod   time.Duration
	DelExpiredPeriod time.Duration
	RmFilesPeriod    time.Duration
}

func (c *SchedulerConfig) EmptyToDefault() {
	if c.LogsTTL == 0 {
		c.LogsTTL = time.Hour * 24 * 30
	}

	if c.AligningPeriod == 0 {
		c.AligningPeriod = time.Minute
	}

	if c.DelExpiredPeriod == 0 {
		c.DelExpiredPeriod = time.Hour
	}

	if c.RmFilesPeriod == 0 {
		c.RmFilesPeriod = time.Minute
	}
}
