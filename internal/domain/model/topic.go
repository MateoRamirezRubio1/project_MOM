package model

type Topic struct {
	Name       string
	Partitions int
	Creator    string
}

func (t Topic) IsValid() bool {
	return t.Name != "" && t.Partitions > 0
}
