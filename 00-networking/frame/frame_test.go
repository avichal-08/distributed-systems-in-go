package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/iotest"
)

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: []byte{}},
		{name: "ascii", payload: []byte("SET a 1")},
		{name: "binary with nulls and newlines", payload: []byte{0x00, 0x01, '\n', '\r', 0xFF, 0x00}},
		{name: "large 64KiB", payload: bytes.Repeat([]byte("A"), 64<<10)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, tc.payload); err != nil {
				t.Fatalf("WriteFrame failed: %v", err)
			}

			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame failed: %v", err)
			}
			if !bytes.Equal(got, tc.payload) {
				t.Fatalf("payload mismatch: got %q, want %q", got, tc.payload)
			}
		})
	}
}

func TestMultipleFramesCoalesced(t *testing.T) {
	var buf bytes.Buffer
	msgs := [][]byte{
		[]byte("SET a 1"),
		[]byte(""),
		[]byte("GET a"),
	}

	for _, msg := range msgs {
		if err := WriteFrame(&buf, msg); err != nil {
			t.Fatalf("WriteFrame(%q) failed: %v", msg, err)
		}
	}

	for i, want := range msgs {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d: ReadFrame failed: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d: got %q, want %q", i, got, want)
		}
	}

	//next read on a clean frame boundary must return io.EOF
	if _, err := ReadFrame(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF at clean boundary, got %v", err)
	}
}

func TestFragmentedOneByteReads(t *testing.T) {
	var buf bytes.Buffer
	want := []byte("fragmented across every single byte boundary")
	if err := WriteFrame(&buf, want); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// iotest.OneByteReader forces every Read() call to return at most 1 byte,
	// exercising partial reads across both the 4-byte header and the payload
	got, err := ReadFrame(iotest.OneByteReader(&buf))
	if err != nil {
		t.Fatalf("ReadFrame on 1-byte stream failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFailureModes(t *testing.T) {
	t.Run("clean EOF at boundary", func(t *testing.T) {
		_, err := ReadFrame(bytes.NewReader(nil))
		if !errors.Is(err, io.EOF) {
			t.Fatalf("got %v, want io.EOF", err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		// only 2 of 4 header bytes arrive before connection drops
		_, err := ReadFrame(bytes.NewReader([]byte{0x00, 0x00}))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("header only with zero payload bytes", func(t *testing.T) {
		//declares 10-byte payload, then EOF immediately
		var hdr [HeaderSize]byte
		binary.BigEndian.PutUint32(hdr[:], 10)

		_, err := ReadFrame(bytes.NewReader(hdr[:]))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("truncated payload", func(t *testing.T) {
		var buf bytes.Buffer
		var hdr [HeaderSize]byte
		binary.BigEndian.PutUint32(hdr[:], 10)
		buf.Write(hdr[:])
		buf.WriteString("12345") // only 5 of 10 bytes

		_, err := ReadFrame(&buf)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("oversized frame on ReadFrame does not OOM", func(t *testing.T) {
		var hdr [HeaderSize]byte
		binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFFF)

		_, err := ReadFrame(bytes.NewReader(hdr[:]))
		if !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("custom MaxFrameSize boundary", func(t *testing.T) {
		f := Framer{MaxFrameSize: 8}

		var buf bytes.Buffer
		if err := f.WriteFrame(&buf, []byte("12345678")); err != nil {
			t.Fatalf("exact max size should succeed: %v", err)
		}
		if _, err := f.ReadFrame(&buf); err != nil {
			t.Fatalf("reading exact max size should succeed: %v", err)
		}

		// 1 byte over must fail locally without writing anything
		buf.Reset()
		err := f.WriteFrame(&buf, []byte("123456789"))
		if !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
		if buf.Len() != 0 {
			t.Fatalf("oversized WriteFrame wrote %d bytes, want 0", buf.Len())
		}
	})
}

func TestConcurrentNetPipe(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	const count = 100
	errCh := make(chan error, 1)

	go func() {
		for i := range count {
			msg := fmt.Appendf(nil, "frame-%d", i)
			if err := WriteFrame(client, msg); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- client.Close()
	}()

	for i := range count {
		want := fmt.Appendf(nil, "frame-%d", i)
		got, err := ReadFrame(server)
		if err != nil {
			t.Fatalf("frame %d: ReadFrame failed: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d: got %q, want %q", i, got, want)
		}
	}

	if _, err := ReadFrame(server); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after writer close, got %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writer goroutine failed: %v", err)
	}
}

func BenchmarkWriteFrame(b *testing.B) {
	sizes := []int{64, 1024, 64 << 10}
	for _, size := range sizes {
		payload := bytes.Repeat([]byte("x"), size)
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			b.SetBytes(int64(HeaderSize + size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := WriteFrame(io.Discard, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadFrame(b *testing.B) {
	sizes := []int{64, 1024, 64 << 10}
	for _, size := range sizes {
		var encoded bytes.Buffer
		if err := WriteFrame(&encoded, bytes.Repeat([]byte("x"), size)); err != nil {
			b.Fatal(err)
		}
		raw := encoded.Bytes()

		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			var r bytes.Reader
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				r.Reset(raw)
				if _, err := ReadFrame(&r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
