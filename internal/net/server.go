// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/zyhnesmr/godis/internal/config"
	"github.com/zyhnesmr/godis/pkg/log"
)

// Server represents the TCP server
type Server struct {
	config   *config.Config
	listener net.Listener
	conns    map[net.Conn]*Conn
	connsMu  sync.RWMutex
	handler  Handler
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// Connection management
	maxClients int
	activeConn int

	// Event hooks
	onConnAccept func(*Conn)
	onConnClose  func(*Conn)
}

// Handler handles connection events
type Handler interface {
	// Handle is called when a connection is established
	Handle(ctx context.Context, conn *Conn)
}

// CommandHandler handles command execution
type CommandHandler interface {
	// Dispatch processes a command and returns the response
	Dispatch(ctx context.Context, conn *Conn, cmdName string, args []string) ([]byte, error)
}

// commandProcessorAdapter adapts CommandHandler to CommandProcessor
type commandProcessorAdapter struct {
	handler CommandHandler
}

func (a *commandProcessorAdapter) ProcessCommand(ctx context.Context, conn *Conn, cmd string, args []string) ([]byte, error) {
	return a.handler.Dispatch(ctx, conn, cmd, args)
}

// NewServer creates a new TCP server
func NewServer(bind string, port int, handler CommandHandler) *Server {
	cfg := config.Instance()
	ctx, cancel := context.WithCancel(context.Background())

	// Create handler adapter - wrap CommandHandler in CommandProcessor adapter, then in Handler
	procAdapter := &commandProcessorAdapter{handler: handler}
	handlerAdapter := &handlerAdapter{processor: procAdapter}

	return &Server{
		config:     cfg,
		conns:      make(map[net.Conn]*Conn),
		handler:    handlerAdapter,
		ctx:        ctx,
		cancel:     cancel,
		maxClients: int(cfg.MaxClients),
	}
}

// handlerAdapter adapts CommandProcessor to Handler interface
type handlerAdapter struct {
	processor CommandProcessor
}

func (a *handlerAdapter) Handle(ctx context.Context, conn *Conn) {
	DefaultHandle(ctx, conn, a.processor)
}

// Start starts the TCP server
func (s *Server) Start(ctx context.Context) error {
	// Use provided context if available
	if ctx != nil {
		s.ctx = ctx
	}
	addr := fmt.Sprintf("%s:%d", s.config.Bind, s.config.Port)
	log.Info("Godis server is now ready to accept connections at %s", addr)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener

	// Check if we can use SO_REUSEPORT
	if tcpL, ok := listener.(*net.TCPListener); ok {
		file, err := tcpL.File()
		if err == nil {
			file.Close()
		}
	}

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the TCP server gracefully
func (s *Server) Stop() {
	log.Info("Server stopping...")

	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all active connections
	s.connsMu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.connsMu.Unlock()

	// Wait for all connections to close
	s.wg.Wait()

	log.Info("Server stopped")
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		rawConn, err := s.listener.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.ctx.Done():
				return
			default:
			}

			// Check for temporary errors
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				continue
				continue
			}

			log.Error("Accept error: %v", err)
			continue
		}

		// Check connection limit
		s.connsMu.Lock()
		if s.maxClients > 0 && len(s.conns) >= s.maxClients {
			s.connsMu.Unlock()
			log.Warn("Max clients reached (%d), rejecting connection from %s", s.maxClients, rawConn.RemoteAddr())
			rawConn.Close()
			continue
		}
		s.connsMu.Unlock()

		// Set TCP keepalive
		if tcpConn, ok := rawConn.(*net.TCPConn); ok {
			if s.config.TCPKeepalive > 0 {
				tcpConn.SetKeepAlive(true)
			}
		}

		// Create connection wrapper
		conn := NewConn(rawConn)

		s.connsMu.Lock()
		s.conns[rawConn] = conn
		s.activeConn = len(s.conns)
		s.connsMu.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single connection
func (s *Server) handleConnection(conn *Conn) {
	defer func() {
		s.wg.Done()

		// Remove from connections map
		s.connsMu.Lock()
		delete(s.conns, conn.rawConn)
		s.activeConn = len(s.conns)
		s.connsMu.Unlock()

		conn.Close()

		if s.onConnClose != nil {
			s.onConnClose(conn)
		}
	}()

	// Call accept hook
	if s.onConnAccept != nil {
		s.onConnAccept(conn)
	}

	log.Debug("New connection from %s", conn.RemoteAddr())

	// Handle connection
	s.handler.Handle(s.ctx, conn)

	log.Debug("Connection closed from %s", conn.RemoteAddr())
}

// GetConnectionCount returns the number of active connections
func (s *Server) GetConnectionCount() int {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	return len(s.conns)
}

// GetConnections returns a copy of active connections
func (s *Server) GetConnections() []*Conn {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()

	conns := make([]*Conn, 0, len(s.conns))
	for _, conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

// CloseConnection closes a specific connection
func (s *Server) CloseConnection(conn *Conn) error {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	if _, ok := s.conns[conn.rawConn]; ok {
		conn.Close()
		delete(s.conns, conn.rawConn)
		s.activeConn = len(s.conns)
		return nil
	}

	return fmt.Errorf("connection not found")
}

// SetConnAcceptHook sets the hook called when a connection is accepted
func (s *Server) SetConnAcceptHook(hook func(*Conn)) {
	s.onConnAccept = hook
}

// SetConnCloseHook sets the hook called when a connection is closed
func (s *Server) SetConnCloseHook(hook func(*Conn)) {
	s.onConnClose = hook
}

// IsRunning returns true if the server is running
func (s *Server) IsRunning() bool {
	select {
	case <-s.ctx.Done():
		return false
	default:
		return true
	}
}

// Context returns the server's context
func (s *Server) Context() context.Context {
	return s.ctx
}
