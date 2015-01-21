package failover

import (
	"github.com/siddontang/go/log"
)

// Failover will do below things after master crashed:
//  1, Promote the best slave which has the up-to-date data with old master.
//  2, Change other slaves's master to the promoted one.
//
// If two or more slaves have same data with master, use the first one now.
func Failover(slaves []*Server) error {
	//need wait all slaves replication done???

	bests, err := findBestSlaves(slaves)
	if err != nil {
		return err
	}

	best := bests[0]

	// promote
	if err = best.Promote(); err != nil {
		return err
	}

	for i := 0; i < len(slaves); i++ {
		if slaves[i] == best {
			continue
		}

		if err = slaves[i].Slaveof(best); err != nil {
			log.Errorf("%s slaveof master %s err %v", slaves[i].Addr, best.Addr, err)
			return err
		}
	}
	return nil
}

func findBestSlaves(slaves []*Server) ([]*Server, error) {
	bestSlaves := []*Server{}

	ps := make([]*ReplStat, len(slaves))

	lastIndex := -1

	for i, slave := range slaves {
		stat, err := slave.ReplicationStat()

		if err != nil {
			return nil, err
		}

		ps[i] = stat

		if lastIndex == -1 {
			lastIndex = i
			bestSlaves = []*Server{slave}
		} else {
			switch ps[lastIndex].Compare(stat) {
			case 1:
				//do nothing
			case -1:
				lastIndex = i
				bestSlaves = []*Server{slave}
			case 0:
				// these two slaves have same data,
				bestSlaves = append(bestSlaves, slave)
			}
		}
	}

	return bestSlaves, nil
}
