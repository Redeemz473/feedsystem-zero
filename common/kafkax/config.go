package kafkax

type ProducerConf struct {
	Brokers      []string
	RequiredAcks int `json:",default=1"`
	BatchSize    int `json:",default=100"`
	BatchBytes   int `json:",default=1048576"`
	FlushMs      int `json:",default=100"`
	RetryMax     int `json:",default=3"`
}

type ConsumerConf struct {
	Brokers        []string
	GroupID        string
	Topics         []string
	MinBytes       int `json:",default=1"`
	MaxBytes       int `json:",default=10485760"`
	MaxWaitMs      int `json:",default=1000"`
	CommitInterval int `json:",default=0"`
	BatchSize      int `json:",default=100"`
	FlushMs        int `json:",default=1000"`
}

func DefaultLocalBrokers() []string {
	return []string{"127.0.0.1:9094"}
}

func (c ProducerConf) Normalize() ProducerConf {
	if len(c.Brokers) == 0 {
		c.Brokers = DefaultLocalBrokers()
	}
	if c.RequiredAcks == 0 {
		c.RequiredAcks = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = 1048576
	}
	if c.FlushMs <= 0 {
		c.FlushMs = 100
	}
	if c.RetryMax <= 0 {
		c.RetryMax = 3
	}
	return c
}

func (c ConsumerConf) Normalize() ConsumerConf {
	if len(c.Brokers) == 0 {
		c.Brokers = DefaultLocalBrokers()
	}
	if c.MinBytes <= 0 {
		c.MinBytes = 1
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 10485760
	}
	if c.MaxWaitMs <= 0 {
		c.MaxWaitMs = 1000
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushMs <= 0 {
		c.FlushMs = 1000
	}
	return c
}
