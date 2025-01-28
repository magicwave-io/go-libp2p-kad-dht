package dht

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	ggio "github.com/gogo/protobuf/io"
	ctxio "github.com/jbenet/go-context/io"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

var dhtReadMessageTimeout = time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

type bufferedWriteCloser interface {
	ggio.WriteCloser
	Flush() error
}

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	ggio.WriteCloser
}

func newBufferedDelimitedWriter(str io.Writer) bufferedWriteCloser {
	w := bufio.NewWriter(str)
	return &bufferedDelimitedWriter{
		Writer:      w,
		WriteCloser: ggio.NewDelimitedWriter(w),
	}
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	defer s.Reset()
	if dht.handleNewMessage(s) {
		// Gracefully close the stream for writes.
		s.Close()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s inet.Stream) bool {
	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := newBufferedDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		var req pb.Message
		switch err := r.ReadMsg(&req); err {
		case io.EOF:
			return true
		default:
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			return false
		case nil:
		}

		startedHandling := time.Now()

		receivedMessages.WithLabelValues(dht.messageLabelValues(&req)...).Inc()
		receivedMessageSizeBytes.WithLabelValues(dht.messageLabelValues(&req)...).Observe(float64(req.Size()))

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			logger.Warningf("can't handle received message of type %v", req.GetType())
			return false
		}

		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			logger.Debugf("error handling message: %v", err)
			return false
		}

		dht.updateFromMessage(ctx, mPeer, &req)

		if resp == nil {
			continue
		}

		// send out response msg
		err = w.WriteMsg(resp)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			logger.Debugf("error writing response: %v", err)
			return false
		}
		inboundRequestHandlingTimeSeconds.WithLabelValues(dht.messageLabelValues(&req)...).Observe(time.Since(startedHandling).Seconds())
	}
}

// Starts a timer for message write latency, and returns a function to be called immediately before
// writing the message.
func (dht *IpfsDHT) beginMessageWriteLatency(ctx context.Context, m *pb.Message) func() {
	now := time.Now()
	return func() {
		messageWriteLatencySeconds.WithLabelValues(dht.messageLabelValues(m)...).Observe(time.Since(now).Seconds())
	}
}

func (dht *IpfsDHT) newStream(ctx context.Context, p peer.ID) (inet.Stream, error) {
	t := time.Now()
	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	if err == nil {
		newStreamTimeSeconds.WithLabelValues(dht.instanceLabelValues()...).Observe(time.Since(t).Seconds())
	} else {
		newStreamTimeErrorSeconds.WithLabelValues(dht.instanceLabelValues()...).Observe(time.Since(t).Seconds())
	}
	return s, err
}

func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, req *pb.Message) (resp *pb.Message, err error) {
	dht.recordOutboundMessage(ctx, req)
	beforeWrite := dht.beginMessageWriteLatency(ctx, req)
	started := time.Now()
	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}
		outboundRequestResponseLatencySeconds.WithLabelValues(
			append(dht.messageLabelValues(req), errStr)...,
		).Observe(time.Since(started).Seconds())
	}()
	ms, err := dht.messageSenderForPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	resp, err = ms.SendRequest(ctx, req, beforeWrite)
	if err != nil {
		return
	}

	// update the peer (on valid msgs only)
	dht.updateFromMessage(ctx, p, resp)

	dht.peerstore.RecordLatency(p, time.Since(start))
	logger.Event(ctx, "dhtReceivedMessage", dht.self, p, resp)
	return resp, nil
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) (err error) {
	dht.recordOutboundMessage(ctx, pmes)
	beforeWrite := dht.beginMessageWriteLatency(ctx, pmes)
	started := time.Now()
	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}
		sendMessageLatencySeconds.WithLabelValues(
			append(dht.messageLabelValues(pmes), errStr)...,
		).Observe(time.Since(started).Seconds())
	}()
	ms, err := dht.messageSenderForPeer(ctx, p)
	if err != nil {
		return err
	}

	if err := ms.SendMessage(ctx, pmes, beforeWrite); err != nil {
		return err
	}
	logger.Event(ctx, "dhtSentMessage", dht.self, p, pmes)
	return nil
}

func (dht *IpfsDHT) recordOutboundMessage(ctx context.Context, m *pb.Message) {
	lvs := dht.messageLabelValues(m)
	sentMessages.WithLabelValues(lvs...).Inc()
	sentMessageSizeBytes.WithLabelValues(lvs...).Observe(float64(m.Size()))
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	// Make sure that this node is actually a DHT server, not just a client.
	protos, err := dht.peerstore.SupportsProtocols(p, dht.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		dht.Update(ctx, p)
	}
	return nil
}

func (dht *IpfsDHT) messageSenderForPeer(ctx context.Context, p peer.ID) (*messageSender, error) {
	dht.smlk.Lock()
	ms, ok := dht.strmap[p]
	if ok {
		dht.smlk.Unlock()
		return ms, nil
	}
	ms = &messageSender{p: p, dht: dht}
	dht.strmap[p] = ms
	dht.smlk.Unlock()

	if err := ms.prepOrInvalidate(ctx); err != nil {
		dht.smlk.Lock()
		defer dht.smlk.Unlock()

		if msCur, ok := dht.strmap[p]; ok {
			// Changed. Use the new one, old one is invalid and
			// not in the map so we can just throw it away.
			if ms != msCur {
				return msCur, nil
			}
			// Not changed, remove the now invalid stream from the
			// map.
			delete(dht.strmap, p)
		}
		// Invalid but not in map. Must have been removed by a disconnect.
		return nil, err
	}
	// All ready to go.
	return ms, nil
}

type messageSender struct {
	s   inet.Stream
	r   ggio.ReadCloser
	w   bufferedWriteCloser
	lk  sync.Mutex
	p   peer.ID
	dht *IpfsDHT

	invalid   bool
	singleMes int
}

// invalidate is called before this messageSender is removed from the strmap.
// It prevents the messageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *messageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		ms.s.Reset()
		ms.s = nil
	}
}

func (ms *messageSender) prepOrInvalidate(ctx context.Context) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(ctx); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *messageSender) prep(ctx context.Context) error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.s != nil {
		return nil
	}

	nstr, err := ms.dht.newStream(ctx, ms.p)
	if err != nil {
		return err
	}

	ms.r = ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	ms.w = newBufferedDelimitedWriter(nstr)
	ms.s = nstr

	return nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (ms *messageSender) SendMessage(ctx context.Context, pmes *pb.Message, beforeWrite func()) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return err
		}

		beforeWrite()
		if err := ms.writeMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Info("error writing message, bailing: ", err)
				return err
			} else {
				logger.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		logger.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go inet.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return nil
	}
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message, beforeWrite func()) (*pb.Message, error) {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return nil, err
		}

		beforeWrite()
		if err := ms.writeMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Info("error writing message, bailing: ", err)
				return nil, err
			} else {
				logger.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		mes := new(pb.Message)
		if err := ms.ctxReadMsg(ctx, mes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Info("error reading message, bailing: ", err)
				return nil, err
			} else {
				logger.Info("error reading message, trying again: ", err)
				retry = true
				continue
			}
		}

		logger.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go inet.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return mes, nil
	}
}

func (ms *messageSender) writeMsg(pmes *pb.Message) error {
	if err := ms.w.WriteMsg(pmes); err != nil {
		return err
	}
	return ms.w.Flush()
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r ggio.ReadCloser) {
		errc <- r.ReadMsg(mes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}
