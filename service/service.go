package service

import (
	"context"
	"net/http"
	"path"
	"time"

	"github.com/google/uuid"
	sl "github.com/j-hitgate/sherlog"
	"github.com/labstack/echo/v4"

	"main/agents/log_utils"
	sa "main/agents/storage"
	aerr "main/app_errors"
	m "main/models"
	"main/relays/file_sys"
)

type Service struct {
	app      *echo.Echo
	config   *m.Config
	metasMap *sa.MetasMap

	writeQueue  chan *m.WriteLogsTask
	readQueue   chan *m.ReadLogsTask
	deleteQueue chan *m.DeleteQuery
	fileSys     *file_sys.FileSys
}

func New(config *m.Config) *Service {
	s := &Service{
		app:      echo.New(),
		config:   config,
		metasMap: sa.NewMetasMap(m.BLOCK_MAX_SIZE),

		writeQueue:  make(chan *m.WriteLogsTask),
		readQueue:   make(chan *m.ReadLogsTask),
		deleteQueue: make(chan *m.DeleteQuery),
		fileSys:     &file_sys.FileSys{},
	}
	s.setRoutes()
	return s
}

func (s *Service) Run() {
	trace := sl.NewTrace("Init")
	defer trace.AddModule("_Service", "Run")()

	// Run transactions/backups and read and clear storages

	file_sys.RunTransactions(trace)

	metasMap, firstRawChunks := s.fileSys.ReadAndClearStorages(trace)

	for storage, metas := range metasMap {
		s.metasMap.AddStorage(trace, storage, metas)
	}

	// Run writers

	for i := byte(0); i < s.config.Writers; i++ {
		sw := sa.NewWriter(m.MAX_LOGS_IN_CHUNK)
		sw.RunWriter(s.writeQueue, uint64(i), firstRawChunks, uint64(s.config.Writers), s.metasMap)
	}

	// Run readers

	for i := byte(0); i < s.config.Readers; i++ {
		sr := sa.NewReader()
		sr.RunReader(s.readQueue, s.metasMap)
	}

	// Run deleters

	sw := sa.NewWriter(m.MAX_LOGS_IN_CHUNK)
	sr := sa.NewReader()

	for i := byte(0); i < s.config.Deleters; i++ {
		sd := sa.NewDeleter(sr, sw)
		sd.RunDeleter(s.deleteQueue, s.metasMap)
	}
	s.fileSys.ReadAndSendDeleteQueries(trace, s.deleteQueue)

	// Run scheduler

	sd := sa.NewDeleter(sr, sw)
	scheduler := sa.NewScheduler(trace, sr, sw, sd, s.config.Scheduler)

	scheduler.RunAligner(s.metasMap)
	scheduler.RunExpiredDeleter(s.metasMap)
	scheduler.RunRemover(s.metasMap)

	err := s.app.Start("127.0.0.1:" + s.config.Port)

	if err != nil && err != http.ErrServerClosed {
		trace.FATAL(nil, "Server error: ", err.Error())
	}
	trace.INFO(nil, "Server closed")
}

func (s *Service) setRoutes() {
	s.app.POST("/logs", s.postLogs)
	s.app.POST("/logs/search", s.postLogsSearch)
	s.app.DELETE("/logs", s.deleteLogs)

	s.app.GET("/storages", s.getStorages)
	s.app.POST("/storage", s.postStorage)
	s.app.DELETE("/storage", s.deleteStorage)

	s.app.POST("/shutdown", s.postShutdown)
}

// Routes

func (s *Service) postLogs(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "PostLogsAPI")
	defer trace.AddModule("_Service", "postLogs")()

	trace.INFO(nil, "Request processing...")

	logs := &m.Logs{}
	err := c.Bind(logs)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	err = logs.Validate(trace)

	if err != nil {
		return s.sendError(c, err)
	}

	task := &m.WriteLogsTask{
		Storage: logs.Storage,
		Logs:    logs.Logs,
		ErrCh:   make(chan error, 1),
		Trace:   trace,
	}
	s.writeQueue <- task
	err = <-task.ErrCh

	if err != nil {
		return s.sendError(c, err)
	}

	trace.INFO(nil, "Request processed")
	return s.sendMessage(c, 201, "Logs saved")
}

func (s *Service) postLogsSearch(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "GetLogsAPI")
	defer trace.AddModule("_Service", "getLogs")()

	trace.INFO(nil, "Request processing...")

	query := &m.SearchQuery{}
	err := c.Bind(query)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	proc, lld, err := log_utils.NewProcessor(trace, query)

	if err != nil {
		return s.sendError(c, err)
	}

	trace.INFO(nil, "Reading and searching logs...")

	task := &m.ReadLogsTask{
		Lld:    lld,
		LogsCh: make(chan []*m.Log, 1),
		ErrCh:  make(chan error, 1),
		Trace:  trace,
	}
	s.readQueue <- task
	err = proc.PutLogsFromChanel(task.LogsCh, task.ErrCh)

	if err != nil {
		return s.sendError(c, err)
	}

	result, err := proc.GetResult()

	if err != nil {
		return s.sendError(c, err)
	}

	trace.INFO(nil, "Request processed")
	return c.JSON(200, result)
}

func (s *Service) deleteLogs(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "DeleteLogsAPI")
	defer trace.AddModule("_Service", "deleteLogs")()

	trace.INFO(nil, "Request processing...")

	query := &m.DeleteQuery{}
	err := c.Bind(query)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	err = query.Validate(trace)

	if err != nil {
		return s.sendError(c, err)
	}

	query.ErrCh = make(chan error, 1)
	query.Trace = trace

	s.deleteQueue <- query
	err = <-query.ErrCh

	if err != nil {
		return s.sendError(c, err)
	}

	trace.INFO(nil, "Request processed")
	return s.sendMessage(c, 200, "Logs will be deleted")
}

func (s *Service) getStorages(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "GetStoragesAPI")
	defer trace.AddModule("_Service", "getStorages")()

	trace.INFO(nil, "Request processing...")

	storages := s.metasMap.Storages()

	trace.INFO(nil, "Request processed")
	return c.JSON(200, storages)
}

func (s *Service) postStorage(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "PostStorageAPI")
	defer trace.AddModule("_Service", "postStorage")()

	trace.INFO(nil, "Request processing...")

	req := &m.Storage{}
	err := c.Bind(req)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	err = req.Validate(trace)

	if err != nil {
		return s.sendError(c, err)
	}

	ok := s.metasMap.AddStorage(trace, req.Storage, []*m.Meta{})

	if !ok {
		err = aerr.NewAppErr(aerr.Conflict, "Storage '", req.Storage, "' already exists")
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}
	s.fileSys.MakeDirAll(trace, "storages", req.Storage)

	trace.INFO(nil, "Request processed")
	return s.sendMessage(c, 201, "Storage created")
}

func (s *Service) deleteStorage(c echo.Context) error {
	trace := sl.NewTrace(uuid.New().String())
	defer trace.Close()
	trace.SetEntity("Request", "DeleteStorageAPI")
	defer trace.AddModule("_Service", "deleteStorage")()

	trace.INFO(nil, "Request processing...")

	req := &m.Storage{}
	err := c.Bind(req)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	err = req.Validate(trace)

	if err != nil {
		return s.sendError(c, err)
	}

	ok := s.metasMap.DeleteStorage(trace, req.Storage)

	if !ok {
		err = aerr.NewAppErr(aerr.NotFound, "Storage '", req.Storage, "' not exists")
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}
	s.fileSys.WriteFile(trace, path.Join("storages", req.Storage, "_deleted_"), false, "")

	trace.INFO(nil, "Request processed")
	return s.sendMessage(c, 200, "Storage deleted")
}

func (s *Service) postShutdown(c echo.Context) (err error) {
	trace := sl.NewTrace(uuid.New().String())
	trace.SetEntity("Request", "PostShutdownAPI")
	popModule := trace.AddModule("_Service", "postShutdown")

	isShutdown := false

	defer func() {
		if isShutdown {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

			err := s.app.Shutdown(ctx)
			cancel()

			if err != nil {
				trace.FATAL(nil, "Shutdown error: ", err.Error())
			}
		}
		popModule()
		trace.Close()
	}()

	trace.INFO(nil, "Request processing...")

	shutdown := &m.Shutdown{}
	err = c.Bind(shutdown)

	if err != nil {
		err = aerr.NewAppErr(aerr.BadReq, "Incorrect format: ", err.(*echo.HTTPError).Message)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	if shutdown.Password != s.config.Password {
		err = aerr.NewAppErr(aerr.Forbidden, "Incorrect password: ", shutdown.Password)
		trace.NOTE(nil, err.Error())
		return s.sendError(c, err)
	}

	isShutdown = true

	trace.INFO(nil, "Request processed")
	return s.sendMessage(c, 200, "Server shutdown")
}

// Messages

func (s *Service) sendError(c echo.Context, err error) error {
	switch err := err.(type) {
	case *aerr.AppErr:
		return s.sendMessage(c, aerr.GetStatus(err.Type()), err.Error())
	default:
		return s.sendMessage(c, 500, "Server error")
	}
}

func (*Service) sendMessage(c echo.Context, status int, msg string) error {
	return c.JSON(status, map[string]string{"error": msg})
}
