package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	//fixed 4byte big-endian payload length prefix
	HeaderSize = 4

	// it caps frame payloads at 16 MiB to prevent OOM attacks or corrupted length headers from exhausting memory
	DefaultMaxFrameSize uint32 = 16 << 20
)

var ErrFrameTooLarge = errors.New("frame: payload exceeds max frame size")

type Framer struct {
	MaxFrameSize uint32
}

func (f Framer) maxFrameSize() uint32 {
	if f.MaxFrameSize == 0 {
		return DefaultMaxFrameSize
	}
	return f.MaxFrameSize
}

// writes a 4byte BE length prefix followed by payload
func (f Framer) WriteFrame(w io.Writer, payload []byte) error {
	maxSize := f.maxFrameSize()
	if uint64(len(payload)) > uint64(maxSize) {
		return fmt.Errorf("%w: got %d bytes, max %d", ErrFrameTooLarge, len(payload), maxSize)
	}

	var header [HeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	bufs := net.Buffers{header[:], payload}
	_, err := bufs.WriteTo(w)
	return err
}

func (f Framer) ReadFrame(r io.Reader) ([]byte, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[:])
	maxSize := f.maxFrameSize()
	if length > maxSize {
		return nil, fmt.Errorf("%w: got %d bytes, max %d", ErrFrameTooLarge, length, maxSize)
	}

	if length == 0 {
		return []byte{}, nil
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		//already consumed a header promising length > 0 bytes; hitting EOF
		// at byte 0 of the payload is a truncated frame, not a clean close
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	return payload, nil
}

func WriteFrame(w io.Writer, payload []byte) error {
	return Framer{}.WriteFrame(w, payload)
}

func ReadFrame(r io.Reader) ([]byte, error) {
	return Framer{}.ReadFrame(r)
}
