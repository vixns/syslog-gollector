package main

import (
	"github.com/vixns/go-syslog"
	"github.com/vixns/syslogparser"
	"github.com/rcrowley/go-metrics"
)

// The ChannelHandler will send all the syslog entries into the given channel
type ChannelHandler struct {
	channel                          syslog.LogPartsChannel

	registry                         metrics.Registry
	parseErrs, messagesRx, bytesRx   metrics.Counter
}

// NewChannelHandler returns a new ChannelHandler
func (ch *ChannelHandler) GetStatistics() (metrics.Registry, error) {
	return ch.registry, nil
}

// NewChannelHandler returns a new ChannelHandler
func NewChannelHandler(channel syslog.LogPartsChannel) *ChannelHandler {
	ch := new(ChannelHandler)

	ch.channel = channel
	ch.parseErrs = metrics.NewCounter()
	ch.messagesRx = metrics.NewCounter()
	ch.bytesRx = metrics.NewCounter()
	ch.registry = metrics.NewRegistry()

	ch.registry.Register("errors.parsing", ch.parseErrs)
	ch.registry.Register("messages.received", ch.messagesRx)
	ch.registry.Register("messages.received.bytes", ch.bytesRx)

	return ch
}

// Syslog entry receiver
func (ch *ChannelHandler) Handle(logParts syslogparser.LogParts, messageLength int64, parseErr error) {
	ch.messagesRx.Inc(1)
	ch.bytesRx.Inc(messageLength)
	if parseErr == nil {
		ch.channel<- logParts
	} else {
		ch.parseErrs.Inc(1)
	}
}
