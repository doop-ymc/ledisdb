package failover

import (
	"fmt"
	"github.com/siddontang/ledisdb/client/go/ledis"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Addr string

	c *ledis.Conn
}

func NewServer(addr string) *Server {
	s := new(Server)

	s.Addr = addr
	s.c = ledis.NewConn(addr)

	return s
}

func (s *Server) Close() {
	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

type ReplStat struct {
	FirstID  uint64
	LastID   uint64
	CommitID uint64
}

func (s *ReplStat) Compare(o *ReplStat) int {
	// bigger last id, more data replicated from master
	if s.LastID > o.LastID {
		return 1
	} else if s.LastID < o.LastID {
		return -1
	} else {
		// smaller first id, less logs purged
		if s.FirstID < o.FirstID {
			return 1
		} else if s.FirstID > o.FirstID {
			return -1
		} else {
			return 0
		}
	}
}

func (s *Server) ReplicationStat() (*ReplStat, error) {
	info, err := ledis.String(s.c.Do("INFO", "REPLICATION"))
	if err != nil {
		return nil, err
	}

	seps := strings.Split(info, "\r\n")
	//skip replication head
	seps = seps[1:]

	stat := new(ReplStat)

	for _, sep := range seps {
		subs := strings.Split(sep, ":")
		switch subs[0] {
		case "last_log_id":
			stat.LastID, err = strconv.ParseUint(subs[0], 10, 64)
		case "first_log_id":
			stat.FirstID, err = strconv.ParseUint(subs[0], 10, 64)
		case "commit_log_id":
			stat.CommitID, err = strconv.ParseUint(subs[0], 10, 64)
		}

		if err != nil {
			return nil, err
		}
	}

	return stat, nil
}

func (s *Server) WaitReplicationDone(seconds int) error {
	if seconds <= 0 {
		seconds = 60
	}

	for i := 0; i < seconds; i++ {
		stat, err := s.ReplicationStat()
		if err != nil {
			return err
		}

		if stat.LastID == stat.CommitID {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("wait replication done timeout")
}

func (s *Server) Slaveof(m *Server) error {
	if m.Addr == s.Addr {
		return fmt.Errorf("slaveof same server %s", m.Addr)
	}

	//host:port
	seps := strings.Split(m.Addr, ":")

	_, err := s.c.Do("SLAVEOF", seps[0], seps[1])
	return err
}

func (s *Server) Promote() error {
	_, err := s.c.Do("SLAVEOF", "NO", "ONE")
	return err
}
