import os
import sys
import logging
import optparse
import datetime
import isolog.core as isolog

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def create_cli():
    cli = optparse.OptionParser()
    cli.add_option('-d', '--daemonize', action='store_true')
    cli.add_option('-c', '--config-file') 
    cli.add_option('-l', '--log-level')
    return cli

def daemonize():
    LOG.info('Daemonizing')
    if hasattr(os, 'devnull'):
        SINK = os.devnull
    else:
        SINK = "/dev/null"

    try:
        pid = os.fork()
        if pid > 0: raise SystemExit
    except OSError:
        LOG.critical('Failed initial fork to background, exiting')
        raise SystemExit, 1        

    os.chdir("/")
    os.setsid()
    os.umask(022)

    try:
        pid = os.fork()
        if pid > 0: raise SystemExit
    except OSError:
        LOG.critical('Failed secondary fork to background, exiting')
        raise SystemExit, 1

    sink = open(SINK, 'rw')
    os.dup2(sink.fileno(), sys.stdin.fileno())
    os.dup2(sink.fileno(), sys.stdout.fileno())
    os.dup2(sink.fileno(), sys.stderr.fileno())

LOGLEVELS = {
    'info': logging.INFO,
    'warn': logging.WARN,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'debug': logging.DEBUG
}

def main(argv=None):
    cli = create_cli()
    opts, args = cli.parse_args(argv)
    
    config = isolog.parse_config(opts.config_file or '/etc/isolog.conf')

    # set up logging to logfile
    log_file = config['main'].get('log', '/var/log/isolog.log')
       
    from isolog.core import LOG as CORELOG
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        '%(asctime)s:%(name)s:%(levelname)s:%(message)s'
    )
    handler.setFormatter(formatter)
    CORELOG.addHandler(handler)
    LOG.addHandler(handler)

    # set logging level
    if opts.log_level:
        level = LOGLEVELS.get(opts.log_level)
        if level: 
            CORELOG.setLevel(level)
            LOG.setLevel(level)

    LOG.info('Started isolog')

    if opts.daemonize: 
        daemonize()
        daemon = True
    else:
        daemon = False
        
    pipes = isolog.generate_pipelines(config)
    threads = isolog.initiate_pipelines(pipes, daemon=daemon)
    
    LOG.info('Workers are now waiting for events.')
    for thread in threads:
        thread.join()
    
    LOG.info('Workers have finished. Exiting')

        
