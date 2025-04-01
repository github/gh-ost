package compress

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zlib"
)

const DefaultCompressionLevel = 6

var (
	zlibReaderPool *sync.Pool
	zlibWriterPool sync.Pool
)

func init() {
	zlibReaderPool = &sync.Pool{
		New: func() interface{} {
			return nil
		},
	}

	zlibWriterPool = sync.Pool{
		New: func() interface{} {
			w, err := zlib.NewWriterLevel(new(bytes.Buffer), DefaultCompressionLevel)
			if err != nil {
				panic(err)
			}
			return w
		},
	}
}

var _ io.WriteCloser = zlibWriter{}
var _ io.ReadCloser = zlibReader{}

type zlibWriter struct {
	w *zlib.Writer
}

type zlibReader struct {
	r io.ReadCloser
}

func GetPooledZlibWriter(target io.Writer) (io.WriteCloser, error) {
	w := zlibWriterPool.Get().(*zlib.Writer)
	w.Reset(target)

	return zlibWriter{
		w: w,
	}, nil
}

func GetPooledZlibReader(src io.Reader) (io.ReadCloser, error) {
	var (
		rc  io.ReadCloser
		err error
	)

	if r := zlibReaderPool.Get(); r != nil {
		rc = r.(io.ReadCloser)
		if rc.(zlib.Resetter).Reset(src, nil) != nil {
			return nil, err
		}
	} else {
		if rc, err = zlib.NewReader(src); err != nil {
			return nil, err
		}
	}

	return zlibReader{
		r: rc,
	}, nil
}

func (c zlibWriter) Write(data []byte) (n int, err error) {
	return c.w.Write(data)
}

func (c zlibWriter) Close() error {
	err := c.w.Close()
	zlibWriterPool.Put(c.w)
	return err
}

func (d zlibReader) Read(buf []byte) (n int, err error) {
	return d.r.Read(buf)
}

func (d zlibReader) Close() error {
	err := d.r.Close()
	zlibReaderPool.Put(d.r)
	return err
}
