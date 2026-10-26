// Package config reads settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env reads variables and collects every problem, so a start fails once with all of them.
type Env struct {
	Problems []string
	lookup   func(string) (string, bool)
}

// New reads from the process's environment.
func New() *Env { return &Env{lookup: os.LookupEnv} }

// FromMap reads from a map (tests).
func FromMap(m map[string]string) *Env {
	return &Env{lookup: func(k string) (string, bool) { v, ok := m[k]; return v, ok }}
}

func (e *Env) get(k string) (string, bool) {
	v, ok := e.lookup(k)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

// Required is a variable that must be set.
func (e *Env) Required(k string) string {
	v, ok := e.get(k)
	if !ok {
		e.Problems = append(e.Problems, k+" is not set")
	}
	return v
}

// String is a variable with a default.
func (e *Env) String(k, def string) string {
	if v, ok := e.get(k); ok {
		return v
	}
	return def
}

// Int is a whole number with a default.
func (e *Env) Int(k string, def int) int {
	v, ok := e.get(k)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.Problems = append(e.Problems, fmt.Sprintf("%s=%q is not a whole number", k, v))
	}
	return n
}

// Int64 is a whole number with a default.
func (e *Env) Int64(k string, def int64) int64 {
	v, ok := e.get(k)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.Problems = append(e.Problems, fmt.Sprintf("%s=%q is not a whole number", k, v))
	}
	return n
}

// Seconds is a duration in whole seconds, 0 (the caller's default) when unset.
func (e *Env) Seconds(k string) time.Duration {
	return time.Duration(e.Int(k, 0)) * time.Second
}

// Bool is true for 1, true, yes.
func (e *Env) Bool(k string, def bool) bool {
	v, ok := e.get(k)
	if !ok {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	e.Problems = append(e.Problems, fmt.Sprintf("%s=%q is not true or false", k, v))
	return def
}

// SecretFile is the trimmed content of the file a variable names.
func (e *Env) SecretFile(k string) string {
	path := e.Required(k)
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		e.Problems = append(e.Problems, fmt.Sprintf("%s: %v", k, err))
		return ""
	}
	v := strings.TrimSpace(string(data))
	if len(v) < 16 {
		e.Problems = append(e.Problems, fmt.Sprintf("%s: the token in %s is shorter than 16 characters", k, path))
	}
	return v
}

// Err is every problem as one error, or nil.
func (e *Env) Err() error {
	if len(e.Problems) == 0 {
		return nil
	}
	return fmt.Errorf("configuration:\n  %s", strings.Join(e.Problems, "\n  "))
}
