package interner

import "sync"

type StringInterner struct {
	m sync.Map // map[string]string
}

func NewStringInterner() *StringInterner {
	return &StringInterner{}
}

func (si *StringInterner) Intern(s string) string {
	if s == "" {
		return ""
	}
	if val, ok := si.m.Load(s); ok {
		return val.(string)
	}
	si.m.Store(s, s)
	return s
}
