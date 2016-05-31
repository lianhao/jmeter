#!/usr/bin/env python

import argparse
import datetime
import random
import socket
import uuid

from oslo_config import cfg
from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import common as rpc_common
from oslo_serialization import jsonutils
from oslo_utils import timeutils
import six
from six import moves

from ceilometer.publisher import utils
from ceilometer import sample


cfg.CONF.register_opt(
    cfg.StrOpt('host',
               default=socket.gethostname(),
               help='Name of this node, which must be valid in an AMQP '
               'key. Can be an opaque identifier. For ZeroMQ only, must '
               'be a valid host name, FQDN, or IP address.'))

def make_test_data(name, meter_type, unit, user_id, project_id,
                   start, end, interval,
                   volume, random_min=-1, random_max=-1,
                   resources_count=1, resource_metadata=None,
                   source='artificial', **kwargs):
    resource_metadata = resource_metadata or {'display_name': 'toto',
                                              'host': 'tata',
                                              'image_ref': 'test',
                                              'instance_flavor_id': 'toto',
                                              'server_group': 'toto',
                                              }
    # Compute start and end timestamps for the new data.
    if isinstance(start, datetime.datetime):
        timestamp = start
    else:
        timestamp = timeutils.parse_strtime(start)

    if not isinstance(end, datetime.datetime):
        end = timeutils.parse_strtime(end)

    increment = datetime.timedelta(seconds=interval)
    resources = [str(uuid.uuid4()) for _ in moves.xrange(resources_count)]
    print('Adding new samples for meter %s.' % (name))
    # Generate samples
    n = 0
    total_volume = volume
    while timestamp <= end:
        if (random_min >= 0 and random_max >= 0):
            # If there is a random element defined, we will add it to
            # user given volume.
            if isinstance(random_min, int) and isinstance(random_max, int):
                total_volume += random.randint(random_min, random_max)
            else:
                total_volume += random.uniform(random_min, random_max)
        resource_id = resources[random.randint(0, len(resources) - 1)]
        c = sample.Sample(name=name,
                          type=meter_type,
                          unit=unit,
                          volume=total_volume,
                          user_id=user_id,
                          project_id=project_id,
                          resource_id=resource_id,
                          timestamp=timestamp.isoformat(),
                          resource_metadata=resource_metadata,
                          source=source,
                          )
        data = utils.meter_message_from_counter(
            c, cfg.CONF.publisher.telemetry_secret)
        yield data
        n += 1
        timestamp = timestamp + increment

        if (meter_type == 'gauge' or meter_type == 'delta'):
            # For delta and gauge, we don't want to increase the value
            # in time by random element. So we always set it back to
            # volume.
            total_volume = volume

    print('Added %d new samples for meter %s.' % (n, name))


def serialize(ctxt, publisher_id, priority, event_type, payload):
    message = {'message_id': six.text_type(uuid.uuid4()),
               'publisher_id': publisher_id,
               'timestamp': timeutils.utcnow(),
               'priority': priority,
               'event_type': event_type,
               'payload': payload,
              }
    rpc_amqp._add_unique_id(message)
    rpc_amqp.pack_context(message, ctxt)
    message = rpc_common.serialize_msg(message)
    return message


def write_to_csv(exchange, routing_key, data, delimeter='|'):
    global csv_file
    content = exchange + delimeter + routing_key + delimeter
    content = content + jsonutils.dumps(data) + "\n"
    csv_file.write(content)


def generate_polling_payload(batch_count, make_data_args):
    batch = []
    def _flush_to_cvs():
        msg = serialize(ctxt={},
                        publisher_id='ceilometer.polling',
                        priority='SAMPLE',
                        event_type='telemetry.polling',
                        payload={'samples': batch})
        write_to_csv('ceilometer', 'notifications.sample', msg)

    for s in make_test_data(**make_data_args.__dict__):
        batch.append(s)
        if len(batch) >= batch_count:
            _flush_to_cvs()
            batch=[]
    if len(batch):
        _flush_to_cvs()
        batch=[]


def generate_pipeline_payload(batch_count, make_data_args):
    batch = []

    def _flush_to_cvs():
        msg = serialize(ctxt={},
                        publisher_id='telemetry.publisher.%s' % cfg.CONF.host,
                        priority='SAMPLE',
                        event_type='metering',
                        payload=batch)
        write_to_csv('ceilometer', 'metering.sample', msg)

    for s in make_test_data(**make_data_args.__dict__):
        batch.append(s)
        if len(batch) >= batch_count:
            _flush_to_cvs()
            batch=[]
    if len(batch):
        _flush_to_cvs()
        batch=[]


csv_file = None

def get_parser():
    parser = argparse.ArgumentParser(
        description='generate data to pumping into message busl',
    )
    parser.add_argument(
        '--file',
        required=True,
        help='csv file to store the generated data',
    )
    parser.add_argument(
        '--type',
        choices=('polling', 'noitifation', 'pipeline'),
        required=True,
        help='types of notificaiton payload to generate'
    )
    parser.add_argument(
        '--batch-count',
        dest='batch_count',
        type=int,
        default=1,
        help='number of samples aggregated in the notification payload',
    )
    parser.add_argument(
        '--interval',
        default=1,
        type=int,
        help='The period between samples, in seconds.',
    )
    parser.add_argument(
        '--start',
        default=31,
        help='Number of days to be stepped back from now or date in the past ('
             '"YYYY-MM-DDTHH:MM:SS" format) to define timestamps start range.',
    )
    parser.add_argument(
        '--end',
        default=2,
        help='Number of days to be stepped forward from now or date in the '
             'future ("YYYY-MM-DDTHH:MM:SS" format) to define timestamps end '
             'range.',
    )
    parser.add_argument(
        '--meter-type',
        choices=sample.TYPES,
        default=sample.TYPE_GAUGE,
        dest='meter_type',
        help='Counter type.',
    )
    parser.add_argument(
        '--unit',
        default=None,
        help='Counter unit.',
    )
    parser.add_argument(
        '--project',
        dest='project_id',
        help='Project id of owner.',
    )
    parser.add_argument(
        '--user',
        dest='user_id',
        help='User id of owner.',
    )
    parser.add_argument(
        '--random_min',
        help='The random min border of amount for added to given volume.',
        type=float,
        default=-1,
    )
    parser.add_argument(
        '--random_max',
        help='The random max border of amount for added to given volume.',
        type=float,
        default=-1,
    )
    parser.add_argument(
        '--resources-count',
        dest='resources_count',
        default=1,
        help='The number of different resources.',
    )
    parser.add_argument(
        '--name',
        default='test_meter',
        dest='name',
        help='The counter name for the meter data.',
    )
    parser.add_argument(
        '--volume',
        help='The amount to attach to the meter.',
        type=float,
        default=1.0,
    )
    return parser


FUNC_MAP={
'polling': generate_polling_payload,
'pipeline': generate_pipeline_payload
}

def main():
    global csv_file

    cfg.CONF([], project='ceilometer')
    args = get_parser().parse_known_args()[0]

    format = '%Y-%m-%dT%H:%M:%S'
    try:
        start = datetime.datetime.utcnow() - datetime.timedelta(
            days=int(args.start))
    except ValueError:
        try:
            start = datetime.datetime.strptime(args.start, format)
        except ValueError:
            raise
    try:
        end = datetime.datetime.utcnow() + datetime.timedelta(
            days=int(args.end))
    except ValueError:
        try:
            end = datetime.datetime.strptime(args.end, format)
        except ValueError:
            raise
    args.start = start
    args.end = end

    with open(args.file, 'w') as f:
        csv_file = f
        FUNC_MAP[args.type](args.batch_count, args)
    return 0


if __name__ == '__main__':
    main()
