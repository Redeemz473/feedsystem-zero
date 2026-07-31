package seed

import (
	"gorm.io/gorm/clause"
)

// onConflictDoNothing returns a GORM clause that ignores duplicate-key errors
// on the given unique columns. This makes seed idempotent: rerunning with the
// same -users / -videos is a no-op instead of a hard failure.
func onConflictDoNothing(columns ...string) clause.OnConflict {
	cols := make([]clause.Column, 0, len(columns))
	for _, c := range columns {
		cols = append(cols, clause.Column{Name: c})
	}
	return clause.OnConflict{
		Columns:   cols,
		DoNothing: true,
	}
}
