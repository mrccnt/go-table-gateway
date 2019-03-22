// Copyright 2019 Marco Conti
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tgw

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
)

// Struct tags
const (
	tagDB      = "db"
	tagTGW     = "tgw"
	tgwPrimary = "primary"
	tgwWrite   = "write"
)

// Gateway is the main struct
type Gateway struct {
	dbx   *sqlx.DB
	table string
}

// tabMeta stores informations about given struct
type tabMeta struct {
	PrimaryName string
	PrimaryDB   string
	WriteCols   []string
	SkipCols    []string
}

// Errors...
var (
	ErrStructConfig = errors.New("invalid or incomplete tags for given struct")
	ErrNoPrimary    = errors.New("no primary key found")
	ErrMultiPrimary = errors.New("multiple primary keys not yet supported")
)

// NewGateway returns a new instance of Gateway
func NewGateway(dbconn *sqlx.DB, table string) (*Gateway, error) {
	return &Gateway{
		table: table,
		dbx:   dbconn,
	}, nil
}

// Create writes entity to database
func (g *Gateway) Create(dest interface{}) error {

	destcfg, err := parseMeta(dest)
	if err != nil {
		return err
	}

	q := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		g.table,
		strings.Join(quoteIdents(destcfg.WriteCols), ","),
		strings.Join(quoteNamedValues(destcfg.WriteCols), ","),
	)

	res, err := g.dbx.NamedExec(q, dest)
	if err != nil {
		fmt.Println("[SQLERR]", "[CREATE]", q)
		return err
	}

	insertID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	reflect.ValueOf(dest).Elem().FieldByName(destcfg.PrimaryName).SetUint(uint64(insertID))

	return nil
}

// Read returns entity with given ID from database
func (g *Gateway) Read(dest interface{}) error {

	destcfg, err := parseMeta(dest)
	if err != nil {
		return err
	}

	q := fmt.Sprintf(
		"SELECT * FROM `%s` WHERE `%s` = ?",
		g.table,
		destcfg.PrimaryDB,
	)

	err = g.dbx.Get(dest, q, getPriVal(dest, destcfg))

	if err != nil {
		fmt.Println("[SQLERR]", "[READ]", q)
		return err
	}

	return nil
}

// Update updates entity in database
func (g *Gateway) Update(dest interface{}) error {

	destcfg, err := parseMeta(dest)
	if err != nil {
		return err
	}

	q := fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE `%s` = :%s",
		g.table,
		strings.Join(quoteUpdateSet(destcfg.WriteCols), ","),
		destcfg.PrimaryDB,
		destcfg.PrimaryDB,
	)

	_, err = g.dbx.NamedExec(q, dest)

	if err != nil {
		fmt.Println("[SQLERR]", "[UPDATE]", q)
		return err
	}

	return nil
}

// Delete removes entity with given ID from database
func (g *Gateway) Delete(dest interface{}) error {

	destcfg, err := parseMeta(dest)
	if err != nil {
		return err
	}

	q := fmt.Sprintf(
		"DELETE FROM `%s` WHERE `%s` = ?",
		g.table,
		destcfg.PrimaryDB,
	)

	_, err = g.dbx.Exec(q, getPriVal(dest, destcfg))

	if err != nil {
		fmt.Println("[SQLERR]", "[DELETE]", q)
		return err
	}

	return nil
}

// Select is a simple select interface using a map as query parameters.
func (g *Gateway) Select(dest interface{}, params map[string]interface{}) error {

	//noinspection GoPreferNilSlice
	args := []interface{}{}

	//noinspection GoPreferNilSlice
	names := []string{}

	for k, v := range params {
		args = append(args, v)
		names = append(names, k)
	}

	q := fmt.Sprintf("SELECT * FROM `%s` WHERE %s", g.table, strings.Join(quoteSelectSet(names), " AND "))

	err := g.dbx.Select(dest, q, args...)
	if err != nil {
		return err
	}

	return nil
}

// getPriVal returns given interfaces primary key value
func getPriVal(dest interface{}, destcfg *tabMeta) uint64 {
	r := reflect.ValueOf(dest).Elem()
	f := reflect.Indirect(r).FieldByName(destcfg.PrimaryName)
	return f.Uint()
}

// quoteIdents decorates given array by quoting query elements
func quoteIdents(names []string) []string {
	//noinspection GoPreferNilSlice
	n := []string{}
	for _, name := range names {
		n = append(n, fmt.Sprintf("`%s`", name))
	}
	return n
}

// quoteUpdateSet decorates given key value pairs by quoting query elements
func quoteUpdateSet(names []string) []string {
	//noinspection GoPreferNilSlice
	n := []string{}
	for _, name := range names {
		n = append(n, fmt.Sprintf("`%s` = :%s", name, name))
	}
	return n
}

// quoteSelectSet decorates given key value pairs by quoting query elements
func quoteSelectSet(names []string) []string {
	//noinspection GoPreferNilSlice
	n := []string{}
	for _, name := range names {
		n = append(n, fmt.Sprintf("`%s` = ?", name))
	}
	return n
}

// quoteNamedValues decorates given array by marking as query placeholder
func quoteNamedValues(names []string) []string {
	//noinspection GoPreferNilSlice
	n := []string{}
	for _, name := range names {
		n = append(n, fmt.Sprintf(":%s", name))
	}
	return n
}

// parseMeta reads struct and returns config
func parseMeta(dest interface{}) (*tabMeta, error) {

	s := tabMeta{
		PrimaryName: "",
		PrimaryDB:   "",
		WriteCols:   []string{},
		SkipCols:    []string{},
	}

	e := reflect.TypeOf(dest).Elem()
	for x := 0; x < e.NumField(); x++ {

		f := e.Field(x)

		dbname := f.Tag.Get(tagDB)
		ops := strings.Split(f.Tag.Get(tagTGW), ",")

		// Mark only once as primary
		if inArray(tgwPrimary, ops) {
			if s.PrimaryName != "" {
				return nil, ErrMultiPrimary
			}
			s.PrimaryName = f.Name
			s.PrimaryDB = dbname
		}

		// Either write or skip
		if inArray(tgwWrite, ops) {
			s.WriteCols = append(s.WriteCols, dbname)
		} else {
			s.SkipCols = append(s.SkipCols, dbname)
		}
	}

	if s.PrimaryName == "" || s.PrimaryDB == "" {
		return nil, ErrNoPrimary
	}

	if len(s.WriteCols) == 0 {
		return nil, ErrStructConfig
	}

	return &s, nil
}

// inArray checks if given needle is in given haystack
func inArray(needle interface{}, haystack interface{}) bool {
	v := reflect.ValueOf(haystack)
	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Interface() == needle {
			return true
		}
	}
	return false
}
