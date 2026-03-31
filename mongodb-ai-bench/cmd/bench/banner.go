package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

var font = map[rune][]string{
	'M': {
		"██   ██",
		"███ ███",
		"██ █ ██",
		"██   ██",
		"██   ██",
	},
	'O': {
		" █████ ",
		"██   ██",
		"██   ██",
		"██   ██",
		" █████ ",
	},
	'N': {
		"██   ██",
		"███  ██",
		"██ █ ██",
		"██  ███",
		"██   ██",
	},
	'G': {
		" █████ ",
		"██     ",
		"██  ███",
		"██   ██",
		" █████ ",
	},
	'A': {
		" █████ ",
		"██   ██",
		"███████",
		"██   ██",
		"██   ██",
	},
	'I': {
		"███",
		" █ ",
		" █ ",
		" █ ",
		"███",
	},
	'-': {
		"     ",
		"     ",
		"█████",
		"     ",
		"     ",
	},
	'B': {
		"██████ ",
		"██   ██",
		"██████ ",
		"██   ██",
		"██████ ",
	},
	'E': {
		"██████",
		"██    ",
		"█████ ",
		"██    ",
		"██████",
	},
	'C': {
		" █████ ",
		"██     ",
		"██     ",
		"██     ",
		" █████ ",
	},
	'H': {
		"██   ██",
		"██   ██",
		"███████",
		"██   ██",
		"██   ██",
	},
}

func renderWord(word string, gap int) []string {
	rows := make([]string, 5)
	for i := 0; i < 5; i++ {
		var parts []string
		for _, ch := range word {
			if glyph, ok := font[ch]; ok {
				parts = append(parts, glyph[i])
			}
		}
		rows[i] = strings.Join(parts, strings.Repeat(" ", gap))
	}
	return rows
}

func runeWidth(s string) int {
	return utf8.RuneCountInString(s)
}

func lerpColor(t float64) (int, int, int) {
	r1, g1, b1 := 0, 90, 40
	r2, g2, b2 := 50, 255, 120
	r := r1 + int(float64(r2-r1)*t)
	g := g1 + int(float64(g2-g1)*t)
	b := b1 + int(float64(b2-b1)*t)
	return r, g, b
}

func printColoredRow(row string, padLeft int, totalWidth int) {
	const (
		bold  = "\033[1m"
		reset = "\033[0m"
	)
	fmt.Print(strings.Repeat(" ", padLeft))
	fmt.Print(bold)
	runes := []rune(row)
	for col, ch := range runes {
		t := float64(col) / float64(max(totalWidth-1, 1))
		r, g, b := lerpColor(t)
		if ch == '█' {
			fmt.Printf("\033[38;2;%d;%d;%dm█", r, g, b)
		} else {
			fmt.Print(" ")
		}
	}
	fmt.Println(reset)
}

func printBanner() {
	const (
		reset = "\033[0m"
		gray  = "\033[38;2;80;80;80m"
		white = "\033[38;2;200;200;200m"
		dim   = "\033[38;2;0;140;60m"
	)

	line1 := renderWord("MONGO", 2)
	line2 := renderWord("AI-BENCH", 1)

	w1 := runeWidth(line1[0])
	w2 := runeWidth(line2[0])
	maxW := w2
	if w1 > w2 {
		maxW = w1
	}

	fmt.Println()
	for _, row := range line1 {
		pad := (maxW - runeWidth(row)) / 2
		printColoredRow(row, pad, maxW)
	}
	fmt.Println()
	for _, row := range line2 {
		pad := (maxW - runeWidth(row)) / 2
		printColoredRow(row, pad, maxW)
	}

	fmt.Printf("\n  %s%s%s\n", gray, strings.Repeat("━", maxW-4), reset)

	subtitle := "Write-Heavy AI Chatbot Benchmark for MongoDB"
	sPad := (maxW - len(subtitle)) / 2
	if sPad < 0 {
		sPad = 0
	}
	fmt.Printf("  %s%s%s%s\n", strings.Repeat(" ", sPad), dim, subtitle, reset)

	version := "v0.1.0"
	vPad := (maxW - len(version)) / 2
	if vPad < 0 {
		vPad = 0
	}
	fmt.Printf("  %s%s%s%s\n\n", strings.Repeat(" ", vPad), white, version, reset)
}
