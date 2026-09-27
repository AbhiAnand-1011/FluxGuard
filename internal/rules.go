package internal

type Rule interface {
	Name() string
	Evaluate(events []Event) bool
}

type CountRule struct {
	Threshold int
}

func (r CountRule) Name() string {
	return "high_event_count"
}

func (r CountRule) Evaluate(events []Event) bool {
	return len(events) >= r.Threshold
}

type TotalValueRule struct {
	Threshold float64
}

func (r TotalValueRule) Name() string {
	return "high_total_value"
}

func (r TotalValueRule) Evaluate(events []Event) bool {
	var total float64

	for _, event := range events {
		total += event.Value
	}

	return total >= r.Threshold
}

type RuleEngine struct {
	rules []Rule
}

func NewRuleEngine() *RuleEngine {
	return &RuleEngine{
		rules: []Rule{
			CountRule{Threshold: 5},
			TotalValueRule{Threshold: 5000},
		},
	}
}

func (e *RuleEngine) Evaluate(events []Event) []string {
	var matches []string

	for _, rule := range e.rules {
		if rule.Evaluate(events) {
			matches = append(matches, rule.Name())
		}
	}

	return matches
}
