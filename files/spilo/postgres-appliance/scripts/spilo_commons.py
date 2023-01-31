import logging
import os
import subprocess
import re
import yaml

logger = logging.getLogger('__name__')

RW_DIR = os.environ.get('RW_DIR', '/run')
PATRONI_CONFIG_FILE = os.path.join(RW_DIR, 'postgres.yml')
LIB_DIR = '/usr/lib/postgresql'

# (min_version, max_version, shared_preload_libraries, extwlist.extensions)
extensions = {
    'columnar': (13, 14, True,  True),
    #'timescaledb':    (9.6, 14, True,  True),
    'pg_cron':        (9.5, 15, True,  True),
    'pg_stat_kcache': (9.4, 15, True,  False),
    'pg_partman':     (9.4, 15, False, True)
}
if os.environ.get('ENABLE_PG_MON') == 'true':
    extensions['pg_mon'] = (11,  15, True,  False)


def adjust_extensions(old, version, extwlist=False):
    ret = []
    for name in old.split(','):
        name = name.strip()
        value = extensions.get(name)
        if name not in ret and value is None or value[0] <= version <= value[1] and (not extwlist or value[3]):
            ret.append(name)
    return ','.join(ret)


def append_extensions(old, version, extwlist=False):
    extwlist = 3 if extwlist else 2
    ret = []

    def maybe_append(name):
        value = extensions.get(name)
        if name not in ret and (value is None or value[0] <= version <= value[1] and value[extwlist]):
            ret.append(name)

    for name in old.split(','):
        maybe_append(name.strip())

    for name in extensions.keys():
        maybe_append(name)

    return ','.join(ret)


def get_binary_version(bin_dir):
    postgres = os.path.join(bin_dir or '', 'postgres')
    version = subprocess.check_output([postgres, '--version']).decode()
    version = re.match(r'^[^\s]+ [^\s]+ (\d+)(\.(\d+))?', version)
    return '.'.join([version.group(1), version.group(3)]) if int(version.group(1)) < 10 else version.group(1)


def get_bin_dir(version):
    return '{0}/{1}/bin'.format(LIB_DIR, version)


def is_valid_pg_version(version):
    bin_dir = get_bin_dir(version)
    postgres = os.path.join(bin_dir, 'postgres')
    # check that there is postgres binary inside
    return os.path.isfile(postgres) and os.access(postgres, os.X_OK)


def write_file(config, filename, overwrite):
    if not overwrite and os.path.exists(filename):
        logger.warning('File %s already exists, not overwriting. (Use option --force if necessary)', filename)
    else:
        with open(filename, 'w') as f:
            logger.info('Writing to file %s', filename)
            f.write(config)


def get_patroni_config():
    with open(PATRONI_CONFIG_FILE) as f:
        return yaml.safe_load(f)


def write_patroni_config(config, force):
    write_file(yaml.dump(config, default_flow_style=False, width=120), PATRONI_CONFIG_FILE, force)
