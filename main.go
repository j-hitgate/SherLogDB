package main

import (
	"log"
	"os"
	"strconv"

	sl "github.com/j-hitgate/sherlog"
	"github.com/joho/godotenv"

	"main/agents/time_range"
	m "main/models"
	"main/service"
)

func main() {
	config := getConfig()

	sl.Init(sl.Config{
		LogsDir:       config.LogsDir,
		AutodumpAfter: 20,
		Level:         config.LogLevel,
	}, nil)

	service.New(config).Run()
	sl.Close()
}

func getConfig() *m.Config {
	err := godotenv.Load()

	if err != nil {
		log.Fatalln("Read '.env' file error: ", err.Error())
	}

	// Get vars

	port := os.Getenv("PORT")
	writersStr := os.Getenv("WRITERS")
	readersStr := os.Getenv("READERS")
	deletersStr := os.Getenv("DELETERS")
	password := os.Getenv("PASSWORD")
	logLevelStr := os.Getenv("DB_LOG_LEVEL")
	logsDir := os.Getenv("DB_LOGS_DIR")

	logsTTLStr := os.Getenv("LOGS_TTL")
	aligningPeriodStr := os.Getenv("ALIGNING_CHUNKS_PERIOD")
	delExpiredPeriodStr := os.Getenv("DELETING_EXPIRED_CHUNKS_PERIOD")
	rmFilesPeriodStr := os.Getenv("REMOVING_FILES_PERIOD")

	// Parse vars

	_, err = strconv.ParseUint(port, 10, 16)

	if err != nil {
		log.Fatalln("PORT must be an integer from 0 to 65 535: ", port)
	}

	writers, err := strconv.ParseUint(writersStr, 10, 8)

	if err != nil {
		log.Fatalln("WRITERS must be an integer from 0 to 255: ", writersStr)
	}

	readers, err := strconv.ParseUint(readersStr, 10, 8)

	if err != nil {
		log.Fatalln("READERS must be an integer from 0 to 255: ", readersStr)
	}

	deleters, err := strconv.ParseUint(deletersStr, 10, 8)

	if err != nil {
		log.Fatalln("DELETERS must be an integer from 0 to 255: ", deletersStr)
	}

	if password == "" {
		log.Fatalln("PASSWORD not specified")
	}

	logLevel, err := strconv.ParseUint(logLevelStr, 10, 8)

	if err != nil {
		log.Fatalln("DB_LOG_LEVEL must be an integer from 0 to 255: ", logLevelStr)
	}

	// For scheduler

	trp := time_range.NewParser(nil)

	logsTTL, err := trp.ParseDuration(logsTTLStr)

	if err != nil {
		log.Fatalln("LOGS_TTL must be a period: ", err.Error())
	}

	aligningPeriod, err := trp.ParseDuration(aligningPeriodStr)

	if err != nil {
		log.Fatalln("ALIGNING_CHUNKS_PERIOD must be a period: ", err.Error())
	}

	delExpiredPeriod, err := trp.ParseDuration(delExpiredPeriodStr)

	if err != nil {
		log.Fatalln("DELETING_EXPIRED_CHUNKS_PERIOD must be a period: ", err.Error())
	}

	rmFilesPeriod, err := trp.ParseDuration(rmFilesPeriodStr)

	if err != nil {
		log.Fatalln("REMOVING_FILES_PERIOD must be a period: ", err.Error())
	}

	// Create config model

	config := &m.Config{
		Port:     port,
		Writers:  byte(writers),
		Readers:  byte(readers),
		Deleters: byte(deleters),
		Password: password,
		LogLevel: byte(logLevel),
		LogsDir:  logsDir,
		Scheduler: m.SchedulerConfig{
			LogsTTL:          logsTTL,
			AligningPeriod:   aligningPeriod,
			DelExpiredPeriod: delExpiredPeriod,
			RmFilesPeriod:    rmFilesPeriod,
		},
	}
	config.EmptyToDefault()
	return config
}
