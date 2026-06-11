package statsd

import (
	"log"
)

func LoggingErrorHandler(err error) {
	if e, ok := err.(*ErrorInputChannelFull); ok {
		log.Printf(
			"Input Queue is full (%d elements): %s %s dropped - %s - increase channel buffer size with `WithChannelModeBufferSize()`",
			e.ChannelSize, e.Metric.name, e.Metric.tags, e.Msg,
		)
		return
	} else if e, ok := err.(*ErrorSenderChannelFull); ok {
		log.Printf(
			"Sender Queue is full (%d elements): %d metrics dropped - %s - increase sender queue size with `WithSenderQueueSize()`",
			e.ChannelSize, e.LostElements, e.Msg,
		)
	} else {
		log.Printf("Error: %v", err)
	}
}
