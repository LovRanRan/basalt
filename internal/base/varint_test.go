package base_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

func TestLengthPrefixedRoundtrip(t *testing.T) {
	values := [][]byte{
		nil,
		{},
		[]byte("v"),
		bytes.Repeat([]byte{0xab}, 300),
		bytes.Repeat([]byte{0x00}, 17000),
	}
	var buf []byte
	for _, v := range values {
		buf = base.AppendLengthPrefixed(buf, v)
	}
	rest := buf
	for i, want := range values {
		var got []byte
		var err error
		got, rest, err = base.DecodeLengthPrefixed(rest)
		if err != nil {
			t.Fatalf("value %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value %d: got %d bytes, want %d bytes", i, len(got), len(want))
		}
	}
	if len(rest) != 0 {
		t.Fatalf("%d trailing bytes after decoding all values", len(rest))
	}
}

func TestDecodeLengthPrefixedCorruption(t *testing.T) {
	cases := []struct {
		name string
		src  []byte
	}{
		{"empty", nil},
		{"truncated varint", []byte{0x80}},
		{"overlong varint", bytes.Repeat([]byte{0x80}, 11)},
		{"length past end", []byte{0x05, 'a', 'b'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := base.DecodeLengthPrefixed(tc.src); !errors.Is(err, base.ErrCorruptFraming) {
				t.Fatalf("err = %v, want ErrCorruptFraming", err)
			}
		})
	}
}

func TestDecodeLengthPrefixedValueAliasIsCapped(t *testing.T) {
	buf := base.AppendLengthPrefixed(nil, []byte("aa"))
	buf = base.AppendLengthPrefixed(buf, []byte("bb"))
	v1, rest, err := base.DecodeLengthPrefixed(buf)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(v1, 'x')
	v2, _, err := base.DecodeLengthPrefixed(rest)
	if err != nil || !bytes.Equal(v2, []byte("bb")) {
		t.Fatalf("append through first value corrupted the second: %q, err = %v", v2, err)
	}
}
