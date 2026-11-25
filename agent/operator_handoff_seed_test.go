package agent

import "strings"
import "testing"

// Both executor seeds keep routine diagnostics inside the authorized runtime
// and reserve a blocking handoff for input only the operator can supply.
func TestPromptSeedsCarryOperatorHandoff(t *testing.T) {
	for name, tmpl := range BuiltinPromptSeeds() {
		switch name {
		case "worker", "pentest":
			for _, marker := range []string{
				"one bounded diagnostic",
				"Keep the configured routing, TLS verification, scope, and permissions",
				"Do not ask the operator to run routine network diagnostics",
			} {
				if !strings.Contains(tmpl, marker) {
					t.Errorf("%s seed missing operator-handoff marker %q", name, marker)
				}
			}
		}
	}
}
