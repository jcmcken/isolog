import time
import os
import re
import threading
import logging
from ConfigParser import RawConfigParser

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def parse_config(filename):
    LOG.info('Parsing config file "%s"' % filename)
    parser = RawConfigParser()
    parser.read(filename)
    conf = parser._sections
    for section, items in conf.iteritems():
        if conf[section].has_key('__name__'):
            del conf[section]['__name__']
    return conf

def validate_pipe(pipe, config):
    assert pipe['source'] in config['sources'].keys()
    assert pipe['endpoint'] in config['endpoints'].keys()
    for filter in pipe['filters']:
        assert filter in config['filters'].keys()

def generate_pipelines(config):
    LOG.info('Generating pipelines')
    pipelines = []
    for pipeline, data in config['pipelines'].iteritems():
        LOG.debug('Parsing pipeline data for "%s"' % pipeline)
        pipe = parse_pipeline(data)
        LOG.debug('Validating pipeline "%s"' % pipeline)
        validate_pipe(pipe, config)
        source = config['sources'].get(pipe['source'])
        endpoint = config['endpoints'].get(pipe['endpoint'])
        filters = [ parse_filter_string(config['filters'].get(f)) for \
            f in pipe['filters'] ]
        pipelines.append({
            'name': pipeline,
            'source': source, 'endpoint': endpoint, 'filters': filters
        })
    return pipelines

def initiate_pipelines(pipelines):
    LOG.info('Initializing pipelines.. getting ready to do work')
    threads = []
    for pipe in pipelines:
        LOG.debug('Initializing pipeline worker "%s"' % pipe['name'])
        t = threading.Thread(
            target=pipeline_worker,
            args=(pipe['source'], pipe['filters'], pipe['endpoint'])
        )
        threads.append(t)
        LOG.debug('Starting pipeline worker "%s"' % pipe['name'])
        t.start()

    LOG.info('Workers are now waiting for events.')
    for thread in threads:
        thread.join()

    return threads    

def pipeline_worker(source, filters, endpoint):
    endpoint_fd = open(endpoint, 'a')
    lines = tail_file(open(source))
    lines = ( line for line in lines if line_match(line, filters) )
    for line in lines:
        endpoint_fd.write(line)
        endpoint_fd.flush()
    
def line_match(line, filters=[]):
    for filter in filters:
        if filter.match(line):
            continue
        else:
            return False
    return True

def invalid_file(filename, type='file or directory'):
    raise IOError(2, 'No such %s' % type, filename)

def validate_file(filename):
    if not os.path.isfile(filename):
        invalid_file(filename, 'file')
    return filename

def validate_basedir(filename):
    basedir = os.path.dirname(filename)
    if not os.path.isdir(basedir):
        invalid_file(basedir, 'directory')
    return filename

def parse_pipeline(pipeline_str):
    components = [i.strip() for i in pipeline_str.split('|')]
    source = components[0]
    endpoint = components[-1]
    filters = components[1:-1]
    return {'source': source, 'filters': filters, 'endpoint': endpoint}

def parse_filter_string(filter):
    new_filter = None
    if not filter.startswith('/'):
        new_filter = '/' + filter
    if not filter.endswith('/'):
        new_filter = filter + '/'
    if new_filter:
        raise ValueError, 'invalid regex "%s", did you mean "%s"?' % (
            filter, new_filter
        )
    return re.compile(filter[1:-1])

def tail_file(fd):
    fd.seek(0,2)
    while True:
        line = fd.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line
