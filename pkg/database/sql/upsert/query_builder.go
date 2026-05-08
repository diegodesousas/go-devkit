package upsert

import (
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
)

var (
	ErrInvalidConstraint   = errors.New("must set at least one constraint")
	ErrInsertValuesIsEmpty = errors.New("insert values must not be empty")
	ErrUpdateValuesIsEmpty = errors.New("update values must not be empty")
	ErrIncompatibleOptions = errors.New("incompatible options (WithOnConflictDoNothing, WithOnConflictUpdate)")
)

type ColumnMap map[string]any

type settings struct {
	table                  string
	constraints            []string
	insertValues           ColumnMap
	onConflictUpdateValues ColumnMap
	onConflictDonNothing   bool
}

func (s settings) validate() error {
	if len(s.constraints) == 0 && (len(s.onConflictUpdateValues) > 0 || s.onConflictDonNothing) {
		return ErrInvalidConstraint
	}

	if len(s.insertValues) == 0 {
		return ErrInsertValuesIsEmpty
	}

	if s.onConflictUpdateValues != nil && len(s.onConflictUpdateValues) == 0 {
		return ErrUpdateValuesIsEmpty
	}

	if s.onConflictDonNothing && len(s.onConflictUpdateValues) > 0 {
		return ErrIncompatibleOptions
	}

	return nil
}

type Option func(settings settings) settings

func WithTable(name string) Option {
	return func(settings settings) settings {
		settings.table = name

		return settings
	}
}

func WithConstraints(names ...string) Option {
	return func(settings settings) settings {
		settings.constraints = append(settings.constraints, names...)

		return settings
	}
}

func WithInsertValues(insertValues ColumnMap) Option {
	return func(settings settings) settings {
		settings.insertValues = insertValues

		return settings
	}
}

func WithOnConflictUpdate(updateValues ColumnMap) Option {
	return func(settings settings) settings {
		settings.onConflictUpdateValues = updateValues

		return settings
	}
}

func WithOnConflictDoNothing() Option {
	return func(settings settings) settings {
		settings.onConflictDonNothing = true

		return settings
	}
}

func Build(options ...Option) (query string, args []any, err error) {
	s := settings{}

	for _, option := range options {
		s = option(s)
	}

	if err = s.validate(); err != nil {
		return
	}

	onConflictInstruction := fmt.Sprintf("ON CONFLICT(%s) DO", strings.Join(s.constraints, ","))

	updateBuilder := squirrel.
		Update(" ").
		Prefix(onConflictInstruction).
		SetMap(s.onConflictUpdateValues)

	insertBuilder := squirrel.
		Insert(s.table).
		SetMap(s.insertValues).
		PlaceholderFormat(squirrel.Dollar)

	if s.onConflictDonNothing {
		insertBuilder = insertBuilder.Suffix(onConflictInstruction + " NOTHING")
	}

	if len(s.onConflictUpdateValues) > 0 {
		var onConflictQuery string
		var updateArgs []any

		// I couldn't cause the error in this statement,
		// basically the builder validates if the table name is empty and the SetMap values
		// were set and we've validated these points before, so the error was omitted here.
		onConflictQuery, updateArgs, _ = updateBuilder.ToSql()

		insertBuilder = insertBuilder.Suffix(onConflictQuery, updateArgs...)
	}

	query, args, err = insertBuilder.ToSql()
	if err != nil {
		return
	}

	return
}
