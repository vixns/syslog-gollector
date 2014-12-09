package main

import (
	"github.com/emmanuel/go-syslog"
	"github.com/jeromer/syslogparser"
	"github.com/rcrowley/go-metrics"
)

//The ChannelHandler will send all the syslog entries into the given channel
type InstrumentedChannelHandler struct {
	channel syslog.LogPartsChannel

	registry    metrics.Registry
	parseErrors metrics.Counter
	messagesRx  metrics.Counter
	bytesRx     metrics.Counter
}

//NewChannelHandler returns a new ChannelHandler
func (ch *InstrumentedChannelHandler) GetStatistics() (metrics.Registry, error) {
	return ch.registry, nil
}

//NewChannelHandler returns a new ChannelHandler
func NewInstrumentedChannelHandler(channel syslog.LogPartsChannel) *InstrumentedChannelHandler {
	ch := new(InstrumentedChannelHandler)

	ch.channel = channel
	ch.parseErrors = metrics.NewCounter()
	ch.messagesRx = metrics.NewCounter()
	ch.bytesRx = metrics.NewCounter()
	// ch.connectionsActive = metrics.NewCounter()
	ch.registry = metrics.NewRegistry()
	ch.registry.Register("errors.parsing", ch.parseErrors)
	ch.registry.Register("messages.received", ch.messagesRx)
	ch.registry.Register("messages.received.bytes", ch.bytesRx)
	// ch.registry.Register("connections.Active", s.connectionsActive)

	return ch
}

//The channel to be used
func (ch *InstrumentedChannelHandler) SetChannel(channel syslog.LogPartsChannel) {
	ch.channel = channel
}

//Syslog entry receiver
func (ch *InstrumentedChannelHandler) Handle(logParts syslogparser.LogParts, messageLength int64, parseErr error) {
	ch.messagesRx.Inc(1)
	ch.bytesRx.Inc(messageLength)
	if parseErr != nil {
		ch.parseErrors.Inc(1)
	} else {
		ch.channel <- logParts
	}
}
