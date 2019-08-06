import asyncio
import sys
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

from opentracing.ext import tags
from jaeger_client import Config
from jaeger_client import SpanContext


# refer: https://github.com/yurishkuro/opentracing-tutorial/tree/master/python
def init_tracer(service):
    config = Config(
        config={ # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': True,
            'reporter_batch_size': 1,
        },
        service_name=service,
    )

    # this call also sets opentracing.tracer
    return config.initialize_tracer()


def decode_msg_data(data):
    # https://github.com/jaegertracing/jaeger-client-go/blob/master/propagation.go
    trace_id_high = int.from_bytes(data[  : 8], byteorder='big')
    trace_id_low  = int.from_bytes(data[ 8:16], byteorder='big')
    span_id       = int.from_bytes(data[16:24], byteorder='big')
    parent_id     = int.from_bytes(data[24:32], byteorder='big')
    flags         = data[32]
    msg           = data[37:].decode('utf-8')
    # print('trace_id_high={}, trace_id_low={}, span_id={}, parent_id={}, flags={}'.format( \
    #             hex(trace_id_high), hex(trace_id_low), hex(span_id), hex(parent_id), hex(flags)))

    return SpanContext(trace_id=trace_id_low, span_id=span_id, parent_id=parent_id, flags=flags) , msg

    

async def run(loop):
    nc = NATS()

    await nc.connect("localhost:4222", loop=loop)
    tracer = init_tracer("NATS Subscirber Python")

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data #msg.data.decode()

        
        span_ctx, msg = decode_msg_data(data)
        span_tags = {tags.SPAN_KIND_CONSUMER: tags.SPAN_KIND_RPC_CLIENT, tags.MESSAGE_BUS_DESTINATION: subject}
        print('Reporting span {}:{}:{}:{}'.format(span_ctx.trace_id, span_ctx.span_id, span_ctx.parent_id, span_ctx.flags))
        with tracer.start_active_span('Received Message', child_of=span_ctx, tags=span_tags):
            print('Msg: ', msg)
            print('Send to Jaeger')

    # Simple publisher and async subscriber via coroutine.
    subject = sys.argv[1]
    print("subscribed [{}]".format(subject))
    await nc.subscribe(subject, cb=message_handler)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
