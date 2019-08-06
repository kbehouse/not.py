// Copyright 2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
	// basictracer "github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	// "github.com/opentracing/opentracing-go/ext"
)

var (
	emptyContext = SpanContext{}
)

func usage() {
	log.Fatalf("Usage: subscribe [-s server] [-creds file] [-t] [-n msgs] <subject>")
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Received on [%s]: '%s'", i, m.Subject, string(m.Data))
}

// TraceID represents unique 128bit identifier of a trace
type TraceID struct {
	High, Low uint64
}

// SpanID represents unique 64bit identifier of a span
type SpanID uint64

// SpanContext represents propagated span identity and state
type SpanContext struct {
	// traceID represents globally unique ID of the trace.
	// Usually generated as a random number.
	traceID TraceID

	// spanID represents span ID that must be unique within its trace,
	// but does not have to be globally unique.
	spanID SpanID

	// parentID refers to the ID of the parent span.
	// Should be 0 if the current span is a root span.
	parentID SpanID

	// flags is a bitmap containing such bits as 'sampled' and 'debug'.
	flags byte

	// Distributed Context baggage. The is a snapshot in time.
	baggage map[string]string

	// debugID can be set to some correlation ID when the context is being
	// extracted from a TextMap carrier.
	//
	// See JaegerDebugHeader in constants.go
	debugID string
}

/*
func binExtract(opaqueCarrier interface{}) (opentracing.SpanContext, error) {
	carrier, ok := opaqueCarrier.(io.Reader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}

	// Read the length of marshalled binary. io.ReadAll isn't that performant
	// since it keeps resizing the underlying buffer as it encounters more bytes
	// to read. By reading the length, we can allocate a fixed sized buf and read
	// the exact amount of bytes into it.
	var length uint32
	if err := binary.Read(carrier, binary.BigEndian, &length); err != nil {
		return nil, opentracing.ErrSpanContextCorrupted
	}
	buf := make([]byte, length)
	if n, err := carrier.Read(buf); err != nil {
		if n > 0 {
			return nil, opentracing.ErrSpanContextCorrupted
		}
		return nil, opentracing.ErrSpanContextNotFound
	}

	ctx := wire.TracerState{}
	if err := proto.Unmarshal(buf, &ctx); err != nil {
		return nil, opentracing.ErrSpanContextCorrupted
	}

	return SpanContext{
		TraceID: ctx.TraceId,
		SpanID:  ctx.SpanId,
		Sampled: ctx.Sampled,
		Baggage: ctx.BaggageItems,
	}, nil
}*/

// Extract implements Extractor of BinaryPropagator
func binExtract(abstractCarrier interface{}) (SpanContext, error) {
	carrier, ok := abstractCarrier.(io.Reader)
	if !ok {
		return emptyContext, opentracing.ErrInvalidCarrier
	}
	var ctx SpanContext

	if err := binary.Read(carrier, binary.BigEndian, &ctx.traceID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.spanID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.parentID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.flags); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}

	// Handle the baggage items
	var numBaggage int32
	if err := binary.Read(carrier, binary.BigEndian, &numBaggage); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	// if iNumBaggage := int(numBaggage); iNumBaggage > 0 {
	// 	ctx.baggage = make(map[string]string, iNumBaggage)
	// 	buf := p.buffers.Get().(*bytes.Buffer)
	// 	defer p.buffers.Put(buf)

	// 	var keyLen, valLen int32
	// 	for i := 0; i < iNumBaggage; i++ {
	// 		if err := binary.Read(carrier, binary.BigEndian, &keyLen); err != nil {
	// 			return emptyContext, opentracing.ErrSpanContextCorrupted
	// 		}
	// 		buf.Reset()
	// 		buf.Grow(int(keyLen))
	// 		if n, err := io.CopyN(buf, carrier, int64(keyLen)); err != nil || int32(n) != keyLen {
	// 			return emptyContext, opentracing.ErrSpanContextCorrupted
	// 		}
	// 		key := buf.String()

	// 		if err := binary.Read(carrier, binary.BigEndian, &valLen); err != nil {
	// 			return emptyContext, opentracing.ErrSpanContextCorrupted
	// 		}
	// 		buf.Reset()
	// 		buf.Grow(int(valLen))
	// 		if n, err := io.CopyN(buf, carrier, int64(valLen)); err != nil || int32(n) != valLen {
	// 			return emptyContext, opentracing.ErrSpanContextCorrupted
	// 		}
	// 		ctx.baggage[key] = buf.String()
	// 	}
	// }

	return ctx, nil
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var numMsgs = flag.Int("n", 1, "Exit after N msgs received.")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	tracer, closer := InitTracing("NATS Subscriber")
	opentracing.SetGlobalTracer(tracer)
	defer closer.Close()

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Tracing Subscriber")}
	opts = SetupConnOptions(tracer, opts)

	// Use UserCredentials.
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Connect to NATS.
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	// Process N messages then exit.
	wg := sync.WaitGroup{}
	wg.Add(*numMsgs)

	subj := args[0]

	nc.Subscribe(subj, func(msg *nats.Msg) {
		defer wg.Done()

		// Create new TraceMsg from normal NATS message.
		t := NewTraceMsg(msg)

		// Extract the span context.
		// sc, err := tracer.Extract(opentracing.Binary, t)
		sc, err := binExtract(t)
		if err != nil {
			log.Printf("Extract error: %v", err)
		}

		// fmt.Println("sc:", sc)
		fmt.Printf("traceID:%x, spanID:%x, parentID:%x, flag:%x\n", sc.traceID, sc.spanID, sc.parentID, sc.flags)

		// Setup a span referring to the span context of the incoming NATS message.
		// span := tracer.StartSpan("Received Message", ext.SpanKindConsumer, opentracing.FollowsFrom(sc))
		// ext.MessageBusDestination.Set(span, msg.Subject)
		// defer span.Finish()

		// The rest of t<TraceMsg> that has not been read is the payload.
		fmt.Printf("Received msg: %q\n", t)
	})

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	wg.Wait()
}
