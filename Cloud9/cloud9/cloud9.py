import logging
import traceback
import json
import httplib
import uuid

from pypes.component import Component

log = logging.getLogger(__name__)


class Cloud9(Component):

    # defines the type of component we're creating.
    __metatype__ = 'PUBLISHER'

    def __init__(self):
        # initialize parent class
        Component.__init__(self)
        self.remove_output('out')

        self.set_parameter('host', 'localhost')
        self.set_parameter('port', '2600')
        self.set_parameter('collection', '')

        # cloud9 bulk indexing end point
        self.index_path = '/v1/_bulk/'
        self.headers = {'content-type': 'application/json; charset=utf-8'}

        log.info('Component Initialized: %s' % self.__class__.__name__)

    def run(self):
        while True:

            # grab runtime config variables
            _host = self.get_parameter('host')
            _port = self.get_parameter('port')

            # empty bulk request object
            bulk_request = []

            # {"index": {"_type": "people", "_id": "0794adfe8336dab765581a98b3701200", "_index": "demos"}}
            for packet in self.receive_all('in'):
                try:
                    # empty document object
                    doc = {}
                    _id = None

                    # convert packet
                    for key, vals in packet:
                        # don't include the raw data field
                        if key == 'data':
                            continue

                        # use id field if it exists
                        if key == 'id':
                            _id = vals[0]

                        if packet.is_multivalued(key):
                            doc[key] = vals
                        else:
                            doc[key] = vals[0]

                    if _id is None:
                        _id = uuid.uuid4()

                    # each entry needs associated meta-data
                    meta = {
                        "index": {
                            "_index": packet.get_meta('route'),
                            "_type": packet.get_meta('id'),
                            "_id": str(_id),
                        }
                    }

                    # both objects to JSON
                    routing = json.dumps(meta)
                    content = json.dumps(doc)

                except Exception as e:
                    log.error('Component Failed: %s' % self.__class__.__name__)
                    log.error('Reason: %s' % str(e))
                    log.error(traceback.print_exc())

                else:
                    # add bulk entry to request
                    bulk_request.append(routing)
                    bulk_request.append(content)

            # generate a request string
            payload = '\n'.join(bulk_request)
            log.info('\n' + payload)

            try:
                # send it to cloud9
                conn = httplib.HTTPConnection(_host, int(_port))
                conn.request('PUT', self.index_path, payload.encode('utf-8'), self.headers)
                res = conn.getresponse()
                res.read()
            except Exception as e:
                log.error('Component Failed: %s' % self.__class__.__name__)
                log.error('Reason: %s' % str(e))
                log.error(traceback.print_exc())

            # yield the CPU, allowing another component to run
            self.yield_ctrl()
