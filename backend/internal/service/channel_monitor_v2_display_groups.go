package service

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

func normalizeChannelMonitorV2DisplayGroups(cfg *ChannelMonitorV2Config) error {
	if len(cfg.DisplayGroups) > 100 {
		return fmt.Errorf("%w: at most 100 display groups", ErrChannelMonitorV2InvalidConfig)
	}
	ids, names, members := map[string]bool{}, map[string]bool{}, map[int64]bool{}
	monitored := map[int64]bool{}
	for _, id := range cfg.GroupIDs {
		monitored[id] = true
	}
	for i := range cfg.DisplayGroups {
		g := &cfg.DisplayGroups[i]
		g.ID, g.Name = strings.TrimSpace(g.ID), strings.TrimSpace(g.Name)
		if g.ID == "" || len(g.ID) > 64 || g.Name == "" || utf8.RuneCountInString(g.Name) > 100 || ids[g.ID] || names[g.Name] {
			return fmt.Errorf("%w: display groups need unique IDs and names (max 64/100 characters)", ErrChannelMonitorV2InvalidConfig)
		}
		ids[g.ID], names[g.Name] = true, true
		var err error
		g.GroupIDs, err = normalizeChannelMonitorV2GroupIDs(g.GroupIDs)
		if err != nil {
			return err
		}
		if len(g.GroupIDs) == 0 {
			return fmt.Errorf("%w: display group %s needs members", ErrChannelMonitorV2InvalidConfig, g.Name)
		}
		for _, id := range g.GroupIDs {
			if members[id] {
				return fmt.Errorf("%w: group %d belongs to multiple display groups", ErrChannelMonitorV2InvalidConfig, id)
			}
			if len(monitored) > 0 && !monitored[id] {
				return fmt.Errorf("%w: group %d is not monitored", ErrChannelMonitorV2InvalidConfig, id)
			}
			members[id] = true
		}
	}
	return nil
}
