package config

import "testing"

func TestSlideSecondsDefaultsToWindowSize(t *testing.T) {
	q := Query{WindowType: "tumbling", WindowSize: 60}
	if got := q.SlideSeconds(); got != 60 {
		t.Errorf("SlideSeconds() = %d, want 60", got)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		query   Query
		wantErr bool
	}{
		{"tumbling without slide", Query{WindowType: "tumbling", WindowSize: 60}, false},
		{"tumbling with matching slide", Query{WindowType: "tumbling", WindowSize: 60, Slide: 60}, false},
		{"tumbling with a shorter slide", Query{WindowType: "tumbling", WindowSize: 60, Slide: 30}, true},
		{"sliding overlapping", Query{WindowType: "sliding", WindowSize: 120, Slide: 60}, false},
		{"sliding not dividing the window", Query{WindowType: "sliding", WindowSize: 100, Slide: 30}, false},
		{"sliding degenerate to tumbling", Query{WindowType: "sliding", WindowSize: 60, Slide: 60}, false},
		{"slide wider than the window leaves gaps", Query{WindowType: "sliding", WindowSize: 60, Slide: 90}, true},
		{"negative slide", Query{WindowType: "sliding", WindowSize: 60, Slide: -1}, true},
		{"missing window size", Query{WindowType: "tumbling"}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.query.validate()
			if (err != nil) != c.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}
