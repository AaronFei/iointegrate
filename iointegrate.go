package ioIntegrate

import (
	"io"
	"sync"

	"github.com/google/uuid"
)

/*
 * Integrated Reader diagram
 *
 *                      / Sub Reader
 * Reader -pipe- Writer - Sub Reader
 *                      \ Sub Reader
 *
 *
 * Integrated Writer diagram
 *                      / Sub Writer
 * Writer -pipe- Reader - Sub Writer
 *                      \ Sub Writer
 */

type IntegratedIoReader_t struct {
	Uid                uuid.UUID
	PipeReader         *io.PipeReader
	internalWriter     *io.PipeWriter
	readerList         map[uuid.UUID]io.Reader
	readerListMutex    sync.Mutex
	isReadCollectExist bool
	bufferSize         int
}

type IntegratedIoWriter_t struct {
	Uid                   uuid.UUID
	internalReader        *io.PipeReader
	PipeWriter            *io.PipeWriter
	writerList            map[uuid.UUID]io.Writer
	writerListMutex       sync.Mutex
	isWriteBroadCastExist bool
	bufferSize            int
}

func CreateIntegratedIoReader(bufferSize int) *IntegratedIoReader_t {
	integratedIo := new(IntegratedIoReader_t)

	r, w := io.Pipe()

	integratedIo.PipeReader = r
	integratedIo.internalWriter = w
	integratedIo.Uid = uuid.Must(uuid.NewRandom())
	integratedIo.readerList = map[uuid.UUID]io.Reader{}
	integratedIo.isReadCollectExist = false
	integratedIo.bufferSize = bufferSize

	return integratedIo
}

func CreateIntegratedIoWriter(bufferSize int) *IntegratedIoWriter_t {
	integratedIo := new(IntegratedIoWriter_t)

	r, w := io.Pipe()

	integratedIo.internalReader = r
	integratedIo.PipeWriter = w
	integratedIo.Uid = uuid.Must(uuid.NewRandom())
	integratedIo.writerList = map[uuid.UUID]io.Writer{}
	integratedIo.isWriteBroadCastExist = false
	integratedIo.bufferSize = bufferSize

	return integratedIo
}

func (p *IntegratedIoWriter_t) AddWriter(w io.Writer) uuid.UUID {
	Uid := uuid.Must(uuid.NewRandom())

	p.writerListMutex.Lock()
	p.writerList[Uid] = w
	p.writerListMutex.Unlock()

	go p.writeBroadCast()

	return Uid
}

func (p *IntegratedIoWriter_t) writeBroadCast() {
	if !p.isWriteBroadCastExist {
		p.isWriteBroadCastExist = true

		buffer := make([]byte, p.bufferSize)
		for {
			n, _ := p.internalReader.Read(buffer)

			p.writerListMutex.Lock()
			for _, w := range p.writerList {
				w.Write(buffer[:n])
			}
			p.writerListMutex.Unlock()
		}
	}
}

func (p *IntegratedIoReader_t) AddReader(r io.Reader) uuid.UUID {
	Uid := uuid.Must(uuid.NewRandom())

	p.readerListMutex.Lock()
	p.readerList[Uid] = r
	p.readerListMutex.Unlock()

	go p.readBroadCast(r, Uid)
	return Uid
}

func (p *IntegratedIoReader_t) readBroadCast(r io.Reader, Uid uuid.UUID) {
	buffer := make([]byte, p.bufferSize)
	for {
		p.readerListMutex.Lock()
		_, ok := p.readerList[Uid]
		p.readerListMutex.Unlock()
		if ok {
			n, _ := r.Read(buffer)

			p.readerListMutex.Lock()
			p.internalWriter.Write(buffer[:n])
			p.readerListMutex.Unlock()
		} else {
			break
		}
	}
}

func (p *IntegratedIoWriter_t) RemoveWriter(uid uuid.UUID) {
	p.writerListMutex.Lock()
	delete(p.writerList, uid)
	p.writerListMutex.Unlock()
}

func (p *IntegratedIoReader_t) RemoveReader(uid uuid.UUID) {
	p.readerListMutex.Lock()
	delete(p.readerList, uid)
	p.readerListMutex.Unlock()
}

func (p *IntegratedIoReader_t) CloseAllReader() {
	p.readerListMutex.Lock()
	p.PipeReader.Close()
	p.readerListMutex.Unlock()
}

func (p *IntegratedIoWriter_t) CloseAllWriter() {
	p.writerListMutex.Lock()
	p.PipeWriter.Close()
	p.writerListMutex.Unlock()
}
