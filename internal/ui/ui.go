// Package ui embeds the controller's single-page control panel so the binary is
// fully self-contained (no separate frontend image or asset volume).
package ui

import _ "embed"

// IndexHTML is the control panel served at "/".
//
//go:embed index.html
var IndexHTML []byte
