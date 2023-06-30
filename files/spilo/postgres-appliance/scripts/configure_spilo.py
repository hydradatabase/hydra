#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import re
import os
from json.decoder import JSONDecodeError

import psutil
import socket
import subprocess
import sys
import pwd

from copy import deepcopy
from six.moves.urllib_parse import urlparse
from collections import defaultdict

import yaml
import pystache
import requests

from spilo_commons import RW_DIR, PATRONI_CONFIG_FILE, append_extensions,\
        get_binary_version, get_bin_dir, is_valid_pg_version, write_file, write_patroni_config


PROVIDER_AWS = "aws"
PROVIDER_GOOGLE = "google"
PROVIDER_OPENSTACK = "openstack"
PROVIDER_LOCAL = "local"
PROVIDER_UNSUPPORTED = "unsupported"
USE_KUBERNETES = os.environ.get('KUBERNETES_SERVICE_HOST') is not None
KUBERNETES_DEFAULT_LABELS = '{"application": "spilo"}'
PATRONI_DCS = ('kubernetes', 'zookeeper', 'exhibitor', 'consul', 'etcd3', 'etcd')
AUTO_ENABLE_WALG_RESTORE = ('WAL_S3_BUCKET', 'WALE_S3_PREFIX', 'WALG_S3_PREFIX', 'WALG_AZ_PREFIX', 'WALG_SSH_PREFIX')
WALG_SSH_NAMES = ['WALG_SSH_PREFIX', 'SSH_PRIVATE_KEY_PATH', 'SSH_USERNAME', 'SSH_PORT']


def parse_args():
    sections = ['all', 'patroni', 'pgqd', 'certificate', 'wal-e', 'crontab',
                'pam-oauth2', 'pgbouncer', 'bootstrap', 'standby-cluster', 'log']
    argp = argparse.ArgumentParser(description='Configures Spilo',
                                   epilog="Choose from the following sections:\n\t{}".format('\n\t'.join(sections)),
                                   formatter_class=argparse.RawDescriptionHelpFormatter)

    argp.add_argument('sections', metavar='sections', type=str, nargs='+', choices=sections,
                      help='Which section to (re)configure')
    argp.add_argument('-l', '--loglevel', type=str, help='Explicitly set loglevel')
    argp.add_argument('-f', '--force', help='Overwrite files if they exist', default=False, action='store_true')

    args = vars(argp.parse_args())

    if 'all' in args['sections']:
        args['sections'] = sections
        args['sections'].remove('all')
    args['sections'] = set(args['sections'])

    return args


def adjust_owner(resource, uid=None, gid=None):
    if uid is None:
        uid = pwd.getpwnam('postgres').pw_uid
    if gid is None:
        gid = pwd.getpwnam('postgres').pw_gid
    os.chown(resource, uid, gid)


def link_runit_service(placeholders, name):
    rw_service = os.path.join(placeholders['RW_DIR'], 'service')
    service_dir = os.path.join(rw_service, name)
    if not os.path.exists(service_dir):
        if not os.path.exists(rw_service):
            os.makedirs(rw_service)
        os.symlink(os.path.join('/etc/runit/runsvdir/default', name), service_dir)
        os.makedirs(os.path.join(placeholders['RW_DIR'], 'supervise', name))


def write_certificates(environment, overwrite):
    """Write SSL certificate to files

    If certificates are specified, they are written, otherwise
    dummy certificates are generated and written"""

    ssl_keys = ['SSL_CERTIFICATE', 'SSL_PRIVATE_KEY']
    if set(ssl_keys) <= set(environment):
        logging.info('Writing custom ssl certificate')
        for k in ssl_keys:
            write_file(environment[k], environment[k + '_FILE'], overwrite)
        if 'SSL_CA' in environment:
            logging.info('Writing ssl ca certificate')
            write_file(environment['SSL_CA'], environment['SSL_CA_FILE'], overwrite)
        else:
            logging.info('No ca certificate to write')
        if 'SSL_CRL' in environment:
            logging.info('Writing ssl certificate revocation list')
            write_file(environment['SSL_CRL'], environment['SSL_CRL_FILE'], overwrite)
        else:
            logging.info('No certificate revocation list to write')
    else:
        if os.path.exists(environment['SSL_PRIVATE_KEY_FILE']) and not overwrite:
            logging.warning('Private key already exists, not overwriting. (Use option --force if necessary)')
            return
        openssl_cmd = [
            '/usr/bin/openssl',
            'req',
            '-nodes',
            '-new',
            '-x509',
            '-subj',
            '/CN=spilo.example.org',
            '-keyout',
            environment['SSL_PRIVATE_KEY_FILE'],
            '-out',
            environment['SSL_CERTIFICATE_FILE'],
        ]
        logging.info('Generating ssl self-signed certificate')
        p = subprocess.Popen(openssl_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output, _ = p.communicate()
        logging.debug(output)

    os.chmod(environment['SSL_PRIVATE_KEY_FILE'], 0o600)
    adjust_owner(environment['SSL_PRIVATE_KEY_FILE'], gid=-1)


def write_restapi_certificates(environment, overwrite):
    """Write REST Api SSL certificate to files

    If certificates are specified, they are written, otherwise
    dummy certificates are generated and written"""

    ssl_keys = ['SSL_RESTAPI_CERTIFICATE', 'SSL_RESTAPI_PRIVATE_KEY']
    if set(ssl_keys) <= set(environment):
        logging.info('Writing REST Api custom ssl certificate')
        for k in ssl_keys:
            write_file(environment[k], environment[k + '_FILE'], overwrite)
        if 'SSL_RESTAPI_CA' in environment:
            logging.info('Writing REST Api ssl ca certificate')
            write_file(environment['SSL_RESTAPI_CA'], environment['SSL_RESTAPI_CA_FILE'], overwrite)
        else:
            logging.info('No REST Api ca certificate to write')

        os.chmod(environment['SSL_RESTAPI_PRIVATE_KEY_FILE'], 0o600)
        adjust_owner(environment['SSL_RESTAPI_PRIVATE_KEY_FILE'], gid=-1)


def deep_update(a, b):
    """Updates data structures

    Dicts are merged, recursively
    If "a" and "b" are lists, list "a" is used
    For anything else, the value of a is returned"""

    if type(a) is dict and type(b) is dict:
        for key in b:
            if key in a:
                a[key] = deep_update(a[key], b[key])
            else:
                a[key] = b[key]
        return a
    if type(a) is list and type(b) is list:
        return a

    return a if a is not None else b


TEMPLATE = \
    '''
bootstrap:
  post_init: /scripts/post_init.sh "{{HUMAN_ROLE}}"
  dcs:
    {{#STANDBY_CLUSTER}}
    standby_cluster:
      create_replica_methods:
      {{#STANDBY_WITH_WALE}}
      - bootstrap_standby_with_wale
      {{/STANDBY_WITH_WALE}}
      - basebackup_fast_xlog
      {{#STANDBY_WITH_WALE}}
      restore_command: envdir "{{STANDBY_WALE_ENV_DIR}}" timeout "{{WAL_RESTORE_TIMEOUT}}"
        /scripts/restore_command.sh "%f" "%p"
      {{/STANDBY_WITH_WALE}}
      {{#STANDBY_HOST}}
      host: {{STANDBY_HOST}}
      {{/STANDBY_HOST}}
      {{#STANDBY_PORT}}
      port: {{STANDBY_PORT}}
      {{/STANDBY_PORT}}
      {{#STANDBY_PRIMARY_SLOT_NAME}}
      primary_slot_name: {{STANDBY_PRIMARY_SLOT_NAME}}
      {{/STANDBY_PRIMARY_SLOT_NAME}}
    {{/STANDBY_CLUSTER}}
    ttl: 30
    loop_wait: &loop_wait 10
    retry_timeout: 10
    maximum_lag_on_failover: 33554432
    postgresql:
      use_pg_rewind: true
      use_slots: true
      parameters:
        archive_mode: "on"
        archive_timeout: 1800s
        wal_level: hot_standby
        wal_log_hints: 'on'
        wal_compression: 'on'
        max_wal_senders: 10
        max_connections: {{postgresql.parameters.max_connections}}
        max_replication_slots: 10
        hot_standby: 'on'
        tcp_keepalives_idle: 300
        tcp_keepalives_interval: 30
        log_line_prefix: '%t [%p]: [%l-1] %c %x %d %u %a %h '
        log_checkpoints: 'on'
        log_lock_waits: 'on'
        log_min_duration_statement: 500
        log_autovacuum_min_duration: 0
        log_connections: 'on'
        log_disconnections: 'on'
        log_statement: 'ddl'
        log_temp_files: 0
        track_functions: all
        checkpoint_completion_target: 0.9
        autovacuum_max_workers: 5
        autovacuum_vacuum_scale_factor: 0.05
        autovacuum_analyze_scale_factor: 0.02
  {{#CLONE_WITH_WALE}}
  method: clone_with_wale
  clone_with_wale:
    command: envdir "{{CLONE_WALE_ENV_DIR}}" python3 /scripts/clone_with_wale.py
      --recovery-target-time="{{CLONE_TARGET_TIME}}"
    recovery_conf:
        restore_command: envdir "{{CLONE_WALE_ENV_DIR}}" timeout "{{WAL_RESTORE_TIMEOUT}}"
          /scripts/restore_command.sh "%f" "%p"
        recovery_target_timeline: "{{CLONE_TARGET_TIMELINE}}"
        {{#USE_PAUSE_AT_RECOVERY_TARGET}}
        recovery_target_action: pause
        {{/USE_PAUSE_AT_RECOVERY_TARGET}}
        {{^USE_PAUSE_AT_RECOVERY_TARGET}}
        recovery_target_action: promote
        {{/USE_PAUSE_AT_RECOVERY_TARGET}}
        {{#CLONE_TARGET_TIME}}
        recovery_target_time: "{{CLONE_TARGET_TIME}}"
        {{/CLONE_TARGET_TIME}}
        {{^CLONE_TARGET_INCLUSIVE}}
        recovery_target_inclusive: false
        {{/CLONE_TARGET_INCLUSIVE}}
  {{/CLONE_WITH_WALE}}
  {{#CLONE_WITH_BASEBACKUP}}
  method: clone_with_basebackup
  clone_with_basebackup:
    command: python3 /scripts/clone_with_basebackup.py --pgpass={{CLONE_PGPASS}} --host={{CLONE_HOST}}
      --port={{CLONE_PORT}} --user="{{CLONE_USER}}"
  {{/CLONE_WITH_BASEBACKUP}}
  initdb:
    - encoding: UTF8
    - locale: {{INITDB_LOCALE}}.UTF-8
    - data-checksums
  {{#USE_ADMIN}}
  users:
    {{PGUSER_ADMIN}}:
      password: {{PGPASSWORD_ADMIN}}
      options:
        - createrole
        - createdb
  {{/USE_ADMIN}}
scope: &scope '{{SCOPE}}'
restapi:
  listen: ':{{APIPORT}}'
  connect_address: {{RESTAPI_CONNECT_ADDRESS}}:{{APIPORT}}
  {{#SSL_RESTAPI_CA_FILE}}
  cafile: {{SSL_RESTAPI_CA_FILE}}
  {{/SSL_RESTAPI_CA_FILE}}
  {{#SSL_RESTAPI_CERTIFICATE_FILE}}
  certfile: {{SSL_RESTAPI_CERTIFICATE_FILE}}
  {{/SSL_RESTAPI_CERTIFICATE_FILE}}
  {{#SSL_RESTAPI_PRIVATE_KEY_FILE}}
  keyfile: {{SSL_RESTAPI_PRIVATE_KEY_FILE}}
  {{/SSL_RESTAPI_PRIVATE_KEY_FILE}}
postgresql:
  pgpass: /run/postgresql/pgpass
  use_unix_socket: true
  use_unix_socket_repl: true
  name: '{{instance_data.id}}'
  listen: '*:{{PGPORT}}'
  connect_address: {{instance_data.ip}}:{{PGPORT}}
  data_dir: {{PGDATA}}
  parameters:
    archive_command: {{{postgresql.parameters.archive_command}}}
    logging_collector: 'on'
    log_destination: csvlog
    log_directory: ../pg_log
    log_filename: 'postgresql-%u.log'
    log_file_mode: '0644'
    log_rotation_age: '1d'
    log_truncate_on_rotation: 'on'
    ssl: 'on'
    {{#SSL_CA_FILE}}
    ssl_ca_file: {{SSL_CA_FILE}}
    {{/SSL_CA_FILE}}
    {{#SSL_CRL_FILE}}
    ssl_crl_file: {{SSL_CRL_FILE}}
    {{/SSL_CRL_FILE}}
    ssl_cert_file: {{SSL_CERTIFICATE_FILE}}
    ssl_key_file: {{SSL_PRIVATE_KEY_FILE}}
    shared_preload_libraries: 'bg_mon,pg_stat_statements,pgextwlist,pg_auth_mon,set_user,columnar,pg_cron'
    bg_mon.listen_address: '{{BGMON_LISTEN_IP}}'
    bg_mon.history_buckets: 120
    pg_stat_statements.track_utility: 'off'
    extwlist.extensions: 'btree_gin,btree_gist,citext,extra_window_functions,first_last_agg,hll,\
hstore,hypopg,intarray,ltree,pgcrypto,pgq,pgq_node,pg_ivm,pg_trgm,postgres_fdw,mysql_fdw,multicorn,\
parquet_s3_fdw,vector,tablefunc,uuid-ossp'
    extwlist.custom_path: /scripts
    cron.use_background_workers: 'on'
  pg_hba:
    - local   all             all                                   trust
    {{#PAM_OAUTH2}}
    - hostssl all             +{{HUMAN_ROLE}}    127.0.0.1/32       pam
    {{/PAM_OAUTH2}}
    - host    all             all                127.0.0.1/32       md5
    {{#PAM_OAUTH2}}
    - hostssl all             +{{HUMAN_ROLE}}    ::1/128            pam
    {{/PAM_OAUTH2}}
    - host    all             all                ::1/128            md5
    - local   replication     {{PGUSER_STANDBY}}                    trust
    - hostssl replication     {{PGUSER_STANDBY}} all                md5
    {{^ALLOW_NOSSL}}
    - hostnossl all           all                all                reject
    {{/ALLOW_NOSSL}}
    {{#PAM_OAUTH2}}
    - hostssl all             +{{HUMAN_ROLE}}    all                pam
    {{/PAM_OAUTH2}}
    {{#ALLOW_NOSSL}}
    - host    all             all                all                md5
    {{/ALLOW_NOSSL}}
    {{^ALLOW_NOSSL}}
    - hostssl all             all                all                md5
    {{/ALLOW_NOSSL}}

  {{#USE_WALE}}
  recovery_conf:
    restore_command: envdir "{{WALE_ENV_DIR}}" timeout "{{WAL_RESTORE_TIMEOUT}}"
      /scripts/restore_command.sh "%f" "%p"
  {{/USE_WALE}}
  authentication:
    superuser:
      username: {{PGUSER_SUPERUSER}}
      password: '{{PGPASSWORD_SUPERUSER}}'
    replication:
      username: {{PGUSER_STANDBY}}
      password: '{{PGPASSWORD_STANDBY}}'
  callbacks:
  {{#CALLBACK_SCRIPT}}
    on_start: {{CALLBACK_SCRIPT}}
    on_stop: {{CALLBACK_SCRIPT}}
    on_role_change: '/scripts/on_role_change.sh {{HUMAN_ROLE}} {{CALLBACK_SCRIPT}}'
 {{/CALLBACK_SCRIPT}}
 {{^CALLBACK_SCRIPT}}
    on_role_change: '/scripts/on_role_change.sh {{HUMAN_ROLE}} true'
 {{/CALLBACK_SCRIPT}}
  create_replica_method:
  {{#USE_WALE}}
    - wal_e
  {{/USE_WALE}}
    - basebackup_fast_xlog
  {{#USE_WALE}}
  wal_e:
    command: envdir {{WALE_ENV_DIR}} bash /scripts/wale_restore.sh
    threshold_megabytes: {{WALE_BACKUP_THRESHOLD_MEGABYTES}}
    threshold_backup_size_percentage: {{WALE_BACKUP_THRESHOLD_PERCENTAGE}}
    retries: 2
    no_master: 1
  {{/USE_WALE}}
  basebackup_fast_xlog:
    command: /scripts/basebackup.sh
    retries: 2
{{#STANDBY_WITH_WALE}}
  bootstrap_standby_with_wale:
    command: envdir "{{STANDBY_WALE_ENV_DIR}}" bash /scripts/wale_restore.sh
    threshold_megabytes: {{WALE_BACKUP_THRESHOLD_MEGABYTES}}
    threshold_backup_size_percentage: {{WALE_BACKUP_THRESHOLD_PERCENTAGE}}
    retries: 2
    no_master: 1
{{/STANDBY_WITH_WALE}}
'''


def get_provider():
    provider = os.environ.get('SPILO_PROVIDER')
    if provider:
        if provider in {PROVIDER_AWS, PROVIDER_GOOGLE, PROVIDER_OPENSTACK, PROVIDER_LOCAL}:
            return provider
        else:
            logging.error('Unknown SPILO_PROVIDER: %s', provider)
            return PROVIDER_UNSUPPORTED

    if os.environ.get('DEVELOP', '').lower() in ['1', 'true', 'on']:
        return PROVIDER_LOCAL

    try:
        logging.info("Figuring out my environment (Google? AWS? Openstack? Local?)")
        r = requests.get('http://169.254.169.254', timeout=2)
        if r.headers.get('Metadata-Flavor', '') == 'Google':
            return PROVIDER_GOOGLE
        else:
            # accessible on Openstack, will fail on AWS
            r = requests.get('http://169.254.169.254/openstack/latest/meta_data.json')
            if r.ok:
                # make sure the response is parsable - https://github.com/Azure/aad-pod-identity/issues/943 and
                # https://github.com/zalando/spilo/issues/542
                r.json()
                return PROVIDER_OPENSTACK

            # is accessible from both AWS and Openstack, Possiblity of misidentification if previous try fails
            r = requests.get('http://169.254.169.254/latest/meta-data/ami-id')
            return PROVIDER_AWS if r.ok else PROVIDER_UNSUPPORTED
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        logging.info("Could not connect to 169.254.169.254, assuming local Docker setup")
        return PROVIDER_LOCAL
    except JSONDecodeError:
        logging.info("Could not parse response from 169.254.169.254, assuming local Docker setup")
        return PROVIDER_LOCAL


def get_instance_metadata(provider):
    metadata = {'ip': socket.getaddrinfo(socket.gethostname(), 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0)[0][4][0],
                'id': socket.gethostname(),
                'zone': 'local'}

    if USE_KUBERNETES:
        metadata['ip'] = os.environ.get('POD_IP', metadata['ip'])

    headers = {}
    if provider == PROVIDER_GOOGLE:
        headers['Metadata-Flavor'] = 'Google'
        url = 'http://169.254.169.254/computeMetadata/v1/instance'  # metadata.google.internal
        mapping = {'zone': 'zone'}
        if not USE_KUBERNETES:
            mapping.update({'id': 'id'})
    elif provider == PROVIDER_AWS:
        url = 'http://169.254.169.254/latest/meta-data'
        mapping = {'zone': 'placement/availability-zone'}
        if not USE_KUBERNETES:
            mapping.update({'ip': 'local-ipv4', 'id': 'instance-id'})
    elif provider == PROVIDER_OPENSTACK:
        mapping = {}  # Disable multi-url fetch
        url = 'http://169.254.169.254/openstack/latest/meta_data.json'
        openstack_metadata = requests.get(url, timeout=5).json()
        metadata['zone'] = openstack_metadata['availability_zone']
        if not USE_KUBERNETES:
            # Try get IP via OpenStack EC2-compatible API, if can't then fail back to auto-discovered one.
            metadata['id'] = openstack_metadata['uuid']
            url = 'http://169.254.169.254/2009-04-04/meta-data'
            r = requests.get(url)
            if r.ok:
                mapping.update({'ip': 'local-ipv4', 'id': 'instance-id'})
    else:
        logging.info("No meta-data available for this provider")
        return metadata

    for k, v in mapping.items():
        metadata[k] = requests.get('{}/{}'.format(url, v or k), timeout=2, headers=headers).text

    return metadata


def set_extended_wale_placeholders(placeholders, prefix):
    """ checks that enough parameters are provided to configure cloning or standby with WAL-E """
    for name in ('S3', 'GS', 'GCS', 'SWIFT', 'AZ'):
        if placeholders.get('{0}WALE_{1}_PREFIX'.format(prefix, name)) or\
                name in ('S3', 'GS', 'AZ') and placeholders.get('{0}WALG_{1}_PREFIX'.format(prefix, name)) or\
                placeholders.get('{0}WAL_{1}_BUCKET'.format(prefix, name)) and placeholders.get(prefix + 'SCOPE'):
            break
    else:
        return False
    scope = placeholders.get(prefix + 'SCOPE')
    dirname = 'env-' + prefix[:-1].lower() + ('-' + scope if scope else '')
    placeholders[prefix + 'WALE_ENV_DIR'] = os.path.join(placeholders['RW_DIR'], 'etc', 'wal-e.d', dirname)
    placeholders[prefix + 'WITH_WALE'] = True
    return name


def set_walg_placeholders(placeholders, prefix=''):
    walg_supported = any(placeholders.get(prefix + n) for n in AUTO_ENABLE_WALG_RESTORE +
                         ('WAL_GS_BUCKET', 'WALE_GS_PREFIX', 'WALG_GS_PREFIX'))
    default = placeholders.get('USE_WALG', False)
    placeholders.setdefault(prefix + 'USE_WALG', default)
    for name in ('USE_WALG_BACKUP', 'USE_WALG_RESTORE'):
        value = str(placeholders.get(prefix + name, placeholders[prefix + 'USE_WALG'])).lower()
        placeholders[prefix + name] = 'true' if value == 'true' and walg_supported else None


def get_listen_ip():
    """ Get IP to listen on for things that don't natively support detecting IPv4/IPv6 dualstack """
    def has_dual_stack():
        if hasattr(socket, 'AF_INET6') and hasattr(socket, 'IPPROTO_IPV6') and hasattr(socket, 'IPV6_V6ONLY'):
            sock = None
            try:
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
                import urllib3
                return urllib3.util.connection.HAS_IPV6
            except socket.error as e:
                logging.debug('Error when working with ipv6 socket: %s', e)
            finally:
                if sock:
                    sock.close()
        return False

    info = socket.getaddrinfo(None, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
    # in case dual stack is not supported we want IPv4 to be preferred over IPv6
    info.sort(key=lambda x: x[0] == socket.AF_INET, reverse=not has_dual_stack())
    return info[0][4][0]


def get_placeholders(provider):
    placeholders = dict(os.environ)

    placeholders.setdefault('PGHOME', os.path.expanduser('~'))
    placeholders.setdefault('APIPORT', '8008')
    placeholders.setdefault('BACKUP_SCHEDULE', '0 1 * * *')
    placeholders.setdefault('BACKUP_NUM_TO_RETAIN', '5')
    placeholders.setdefault('CRONTAB', '[]')
    placeholders.setdefault('PGROOT', os.path.join(placeholders['PGHOME'], 'pgroot'))
    placeholders.setdefault('WALE_TMPDIR', os.path.abspath(os.path.join(placeholders['PGROOT'], '../tmp')))
    placeholders.setdefault('PGDATA', os.path.join(placeholders['PGROOT'], 'pgdata'))
    placeholders.setdefault('HUMAN_ROLE', 'zalandos')
    placeholders.setdefault('PGUSER_STANDBY', 'standby')
    placeholders.setdefault('PGPASSWORD_STANDBY', 'standby')
    placeholders.setdefault('USE_ADMIN', 'PGPASSWORD_ADMIN' in placeholders)
    placeholders.setdefault('PGUSER_ADMIN', 'admin')
    placeholders.setdefault('PGPASSWORD_ADMIN', 'cola')
    placeholders.setdefault('PGUSER_SUPERUSER', 'postgres')
    placeholders.setdefault('PGPASSWORD_SUPERUSER', 'zalando')
    placeholders.setdefault('ALLOW_NOSSL', '')
    placeholders.setdefault('BGMON_LISTEN_IP', '0.0.0.0')
    placeholders.setdefault('PGPORT', '5432')
    placeholders.setdefault('SCOPE', 'dummy')
    placeholders.setdefault('RW_DIR', RW_DIR)
    placeholders.setdefault('SSL_TEST_RELOAD', 'SSL_PRIVATE_KEY_FILE' in os.environ)
    placeholders.setdefault('SSL_CA_FILE', '')
    placeholders.setdefault('SSL_CRL_FILE', '')
    placeholders.setdefault('SSL_CERTIFICATE_FILE', os.path.join(placeholders['RW_DIR'], 'certs', 'server.crt'))
    placeholders.setdefault('SSL_PRIVATE_KEY_FILE', os.path.join(placeholders['RW_DIR'], 'certs', 'server.key'))
    placeholders.setdefault('SSL_RESTAPI_CA_FILE', '')
    placeholders.setdefault('SSL_RESTAPI_CERTIFICATE_FILE', '')
    placeholders.setdefault('SSL_RESTAPI_PRIVATE_KEY_FILE', '')
    placeholders.setdefault('WALE_BACKUP_THRESHOLD_MEGABYTES', 102400)
    placeholders.setdefault('WALE_BACKUP_THRESHOLD_PERCENTAGE', 30)
    placeholders.setdefault('INITDB_LOCALE', 'en_US')
    placeholders.setdefault('CLONE_TARGET_TIMELINE', 'latest')
    # if Kubernetes is defined as a DCS, derive the namespace from the POD_NAMESPACE, if not set explicitely.
    # We only do this for Kubernetes DCS, as we don't want to suddently change, i.e. DCS base path when running
    # in Kubernetes with Etcd in a non-default namespace
    placeholders.setdefault('NAMESPACE', placeholders.get('POD_NAMESPACE', 'default')
                            if USE_KUBERNETES and placeholders.get('DCS_ENABLE_KUBERNETES_API') else '')
    # use namespaces to set WAL bucket prefix scope naming the folder namespace-clustername for non-default namespace.
    placeholders.setdefault('WAL_BUCKET_SCOPE_PREFIX', '{0}-'.format(placeholders['NAMESPACE'])
                            if placeholders['NAMESPACE'] not in ('default', '') else '')
    placeholders.setdefault('WAL_BUCKET_SCOPE_SUFFIX', '')
    placeholders.setdefault('WAL_RESTORE_TIMEOUT', '0')
    placeholders.setdefault('WALE_ENV_DIR', os.path.join(placeholders['RW_DIR'], 'etc', 'wal-e.d', 'env'))
    placeholders.setdefault('USE_WALE', False)
    cpu_count = str(min(psutil.cpu_count(), 10))
    placeholders.setdefault('WALG_DOWNLOAD_CONCURRENCY', cpu_count)
    placeholders.setdefault('WALG_UPLOAD_CONCURRENCY', cpu_count)
    placeholders.setdefault('PAM_OAUTH2', '')
    placeholders.setdefault('CALLBACK_SCRIPT', '')
    placeholders.setdefault('DCS_ENABLE_KUBERNETES_API', '')
    placeholders.setdefault('KUBERNETES_ROLE_LABEL', 'spilo-role')
    placeholders.setdefault('KUBERNETES_SCOPE_LABEL', 'version')
    placeholders.setdefault('KUBERNETES_LABELS', KUBERNETES_DEFAULT_LABELS)
    placeholders.setdefault('KUBERNETES_USE_CONFIGMAPS', '')
    placeholders.setdefault('KUBERNETES_BYPASS_API_SERVICE', 'true')
    placeholders.setdefault('USE_PAUSE_AT_RECOVERY_TARGET', False)
    placeholders.setdefault('CLONE_METHOD', '')
    placeholders.setdefault('CLONE_WITH_WALE', '')
    placeholders.setdefault('CLONE_WITH_BASEBACKUP', '')
    placeholders.setdefault('CLONE_TARGET_TIME', '')
    placeholders.setdefault('CLONE_TARGET_INCLUSIVE', True)

    placeholders.setdefault('LOG_SHIP_SCHEDULE', '1 0 * * *')
    placeholders.setdefault('LOG_S3_BUCKET', '')
    placeholders.setdefault('LOG_TMPDIR', os.path.abspath(os.path.join(placeholders['PGROOT'], '../tmp')))
    placeholders.setdefault('LOG_BUCKET_SCOPE_SUFFIX', '')

    # see comment for wal-e bucket prefix
    placeholders.setdefault('LOG_BUCKET_SCOPE_PREFIX', '{0}-'.format(placeholders['NAMESPACE'])
                            if placeholders['NAMESPACE'] not in ('default', '') else '')

    if placeholders['CLONE_METHOD'] == 'CLONE_WITH_WALE':
        # modify placeholders and take care of error cases
        name = set_extended_wale_placeholders(placeholders, 'CLONE_')
        if name is False:
            logging.warning('Cloning with WAL-E is only possible when CLONE_WALE_*_PREFIX '
                            'or CLONE_WALG_*_PREFIX or CLONE_WAL_*_BUCKET and CLONE_SCOPE are set.')
        elif name == 'S3':
            placeholders.setdefault('CLONE_USE_WALG', 'true')
    elif placeholders['CLONE_METHOD'] == 'CLONE_WITH_BASEBACKUP':
        clone_scope = placeholders.get('CLONE_SCOPE')
        if clone_scope and placeholders.get('CLONE_HOST') \
                and placeholders.get('CLONE_USER') and placeholders.get('CLONE_PASSWORD'):
            placeholders['CLONE_WITH_BASEBACKUP'] = True
            placeholders.setdefault('CLONE_PGPASS', os.path.join(placeholders['PGHOME'],
                                                                 '.pgpass_{0}'.format(clone_scope)))
            placeholders.setdefault('CLONE_PORT', 5432)
        else:
            logging.warning("Clone method is set to basebackup, but no 'CLONE_SCOPE' "
                            "or 'CLONE_HOST' or 'CLONE_USER' or 'CLONE_PASSWORD' specified")
    else:
        if set_extended_wale_placeholders(placeholders, 'STANDBY_') == 'S3':
            placeholders.setdefault('STANDBY_USE_WALG', 'true')

    placeholders.setdefault('STANDBY_WITH_WALE', '')
    placeholders.setdefault('STANDBY_HOST', '')
    placeholders.setdefault('STANDBY_PORT', '')
    placeholders.setdefault('STANDBY_PRIMARY_SLOT_NAME', '')
    placeholders.setdefault('STANDBY_CLUSTER', placeholders['STANDBY_WITH_WALE'] or placeholders['STANDBY_HOST'])

    if provider == PROVIDER_AWS and not USE_KUBERNETES:
        # AWS specific callback to tag the instances with roles
        placeholders['CALLBACK_SCRIPT'] = 'python3 /scripts/callback_aws.py'
        if placeholders.get('EIP_ALLOCATION'):
            placeholders['CALLBACK_SCRIPT'] += ' ' + placeholders['EIP_ALLOCATION']

    if any(placeholders.get(n) for n in AUTO_ENABLE_WALG_RESTORE):
        placeholders.setdefault('USE_WALG_RESTORE', 'true')
    if placeholders.get('WALG_AZ_PREFIX'):
        placeholders.setdefault('USE_WALG_BACKUP', 'true')
    if all(placeholders.get(n) for n in WALG_SSH_NAMES):
        placeholders.setdefault('USE_WALG_BACKUP', 'true')
    set_walg_placeholders(placeholders)

    placeholders['USE_WALE'] = any(placeholders.get(n) for n in AUTO_ENABLE_WALG_RESTORE +
                                   ('WAL_SWIFT_BUCKET', 'WALE_SWIFT_PREFIX', 'WAL_GCS_BUCKET',
                                    'WAL_GS_BUCKET', 'WALE_GS_PREFIX', 'WALG_GS_PREFIX'))

    if placeholders.get('WALG_BACKUP_FROM_REPLICA'):
        placeholders['WALG_BACKUP_FROM_REPLICA'] = str(placeholders['WALG_BACKUP_FROM_REPLICA']).lower()

    # Kubernetes requires a callback to change the labels in order to point to the new master
    if USE_KUBERNETES:
        if not placeholders.get('DCS_ENABLE_KUBERNETES_API'):
            placeholders['CALLBACK_SCRIPT'] = 'python3 /scripts/callback_role.py'

    placeholders.setdefault('postgresql', {})
    placeholders['postgresql'].setdefault('parameters', {})
    placeholders['WALE_BINARY'] = 'wal-g' if placeholders.get('USE_WALG_BACKUP') == 'true' else 'wal-e'
    placeholders['postgresql']['parameters']['archive_command'] = \
        'envdir "{WALE_ENV_DIR}" {WALE_BINARY} wal-push "%p"'.format(**placeholders) \
        if placeholders['USE_WALE'] else '/bin/true'

    cgroup_memory_limit_path = '/sys/fs/cgroup/memory/memory.limit_in_bytes'
    cgroup_v2_memory_limit_path = '/sys/fs/cgroup/memory.max'

    if os.path.exists(cgroup_memory_limit_path):
        with open(cgroup_memory_limit_path) as f:
            os_memory_mb = int(f.read()) / 1048576
    elif os.path.exists(cgroup_v2_memory_limit_path):
        with open(cgroup_v2_memory_limit_path) as f:
            try:
                os_memory_mb = int(f.read()) / 1048576
            except Exception:  # string literal "max" is a possible value
                os_memory_mb = 0x7FFFFFFFFFF
    else:
        os_memory_mb = sys.maxsize
    os_memory_mb = min(os_memory_mb, os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1048576)

    # # 1 connection per 30 MB, at least 100, at most 1000
    placeholders['postgresql']['parameters']['max_connections'] = min(max(100, int(os_memory_mb/30)), 1000)

    placeholders['instance_data'] = get_instance_metadata(provider)
    placeholders.setdefault('RESTAPI_CONNECT_ADDRESS', placeholders['instance_data']['ip'])

    placeholders['BGMON_LISTEN_IP'] = get_listen_ip()

    if 'SSL_CA' in placeholders and placeholders['SSL_CA_FILE'] == '':
        placeholders['SSL_CA_FILE'] = os.path.join(placeholders['RW_DIR'], 'certs', 'ca.crt')
    if 'SSL_CRL' in placeholders and placeholders['SSL_CRL_FILE'] == '':
        placeholders['SSL_CRL_FILE'] = os.path.join(placeholders['RW_DIR'], 'certs', 'server.crl')

    if {'SSL_RESTAPI_CERTIFICATE', 'SSL_RESTAPI_PRIVATE_KEY'} <= set(placeholders):
        if not placeholders['SSL_RESTAPI_CERTIFICATE_FILE']:
            placeholders['SSL_RESTAPI_CERTIFICATE_FILE'] = os.path.join(placeholders['RW_DIR'], 'certs',
                                                                        'rest-api-server.crt')
        if not placeholders['SSL_RESTAPI_PRIVATE_KEY_FILE']:
            placeholders['SSL_RESTAPI_PRIVATE_KEY_FILE'] = os.path.join(placeholders['RW_DIR'], 'certs',
                                                                        'restapi-api-server.key')
    if placeholders.get('SSL_RESTAPI_CA') and not placeholders['SSL_RESTAPI_CA_FILE']:
        placeholders['SSL_RESTAPI_CA_FILE'] = os.path.join(placeholders['RW_DIR'], 'certs', 'rest-api-ca.crt')

    return placeholders


def pystache_render(*args, **kwargs):
    render = pystache.Renderer(missing_tags='strict')
    return render.render(*args, **kwargs)


def get_dcs_config(config, placeholders):
    # (KUBERNETES|ZOOKEEPER|EXHIBITOR|CONSUL|ETCD3|ETCD)_(HOSTS|HOST|PORT|...)
    dcs_configs = defaultdict(dict)
    for name, value in placeholders.items():
        if '_' not in name:
            continue
        dcs, param = name.lower().split('_', 1)
        if dcs in PATRONI_DCS:
            if param == 'hosts':
                if not (value.strip().startswith('-') or '[' in value):
                    value = '[{0}]'.format(value)
                value = yaml.safe_load(value)
            elif param == 'discovery_domain':
                param = 'discovery_srv'
            dcs_configs[dcs][param] = value

    if USE_KUBERNETES and placeholders.get('DCS_ENABLE_KUBERNETES_API'):
        config = {'kubernetes': dcs_configs['kubernetes']}
        try:
            kubernetes_labels = json.loads(config['kubernetes'].get('labels'))
        except (TypeError, ValueError) as e:
            logging.warning("could not parse kubernetes labels as a JSON: %r, "
                            "reverting to the default: %s", e, KUBERNETES_DEFAULT_LABELS)
            kubernetes_labels = json.loads(KUBERNETES_DEFAULT_LABELS)
        config['kubernetes']['labels'] = kubernetes_labels

        if not config['kubernetes'].pop('use_configmaps'):
            config['kubernetes'].update({'use_endpoints': True, 'ports': [{'port': 5432, 'name': 'postgresql'}]})
        if str(config['kubernetes'].pop('bypass_api_service', None)).lower() == 'true':
            config['kubernetes']['bypass_api_service'] = True
    else:
        for dcs in PATRONI_DCS:
            if dcs != 'kubernetes' and dcs in dcs_configs:
                config = {dcs: dcs_configs[dcs]}
                break
        else:
            config = {}  # Configuration can also be specified using either SPILO_CONFIGURATION or PATRONI_CONFIGURATION

    if placeholders['NAMESPACE'] not in ('default', ''):
        config['namespace'] = placeholders['NAMESPACE']

    return config


def write_log_environment(placeholders):
    log_env = defaultdict(lambda: '')
    log_env.update(placeholders)

    aws_region = log_env.get('AWS_REGION')
    if not aws_region:
        aws_region = placeholders['instance_data']['zone'][:-1]
    log_env['LOG_AWS_HOST'] = 's3.{}.amazonaws.com'.format(aws_region)

    log_s3_key = 'spilo/{LOG_BUCKET_SCOPE_PREFIX}{SCOPE}{LOG_BUCKET_SCOPE_SUFFIX}/log/'.format(**log_env)
    log_s3_key += placeholders['instance_data']['id']
    log_env['LOG_S3_KEY'] = log_s3_key

    if not os.path.exists(log_env['LOG_TMPDIR']):
        os.makedirs(log_env['LOG_TMPDIR'])
        os.chmod(log_env['LOG_TMPDIR'], 0o1777)

    if not os.path.exists(log_env['LOG_ENV_DIR']):
        os.makedirs(log_env['LOG_ENV_DIR'])

    for var in ('LOG_TMPDIR', 'LOG_AWS_HOST', 'LOG_S3_KEY', 'LOG_S3_BUCKET', 'PGLOG'):
        write_file(log_env[var], os.path.join(log_env['LOG_ENV_DIR'], var), True)


def write_wale_environment(placeholders, prefix, overwrite):
    s3_names = ['WALE_S3_PREFIX', 'WALG_S3_PREFIX', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                'WALE_S3_ENDPOINT', 'AWS_ENDPOINT', 'AWS_REGION', 'AWS_INSTANCE_PROFILE', 'WALE_DISABLE_S3_SSE',
                'WALG_S3_SSE_KMS_ID', 'WALG_S3_SSE', 'WALG_DISABLE_S3_SSE', 'AWS_S3_FORCE_PATH_STYLE', 'AWS_ROLE_ARN',
                'AWS_WEB_IDENTITY_TOKEN_FILE', 'AWS_STS_REGIONAL_ENDPOINTS']
    azure_names = ['WALG_AZ_PREFIX', 'AZURE_STORAGE_ACCOUNT',  'WALG_AZURE_BUFFER_SIZE', 'WALG_AZURE_MAX_BUFFERS',
                   'AZURE_ENVIRONMENT_NAME']
    azure_auth_names = ['AZURE_STORAGE_ACCESS_KEY', 'AZURE_STORAGE_SAS_TOKEN', 'AZURE_CLIENT_ID',
                        'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID']
    gs_names = ['WALE_GS_PREFIX', 'WALG_GS_PREFIX', 'GOOGLE_APPLICATION_CREDENTIALS']
    swift_names = ['WALE_SWIFT_PREFIX', 'SWIFT_AUTHURL', 'SWIFT_TENANT', 'SWIFT_TENANT_ID', 'SWIFT_USER',
                   'SWIFT_USER_ID', 'SWIFT_USER_DOMAIN_NAME', 'SWIFT_USER_DOMAIN_ID', 'SWIFT_PASSWORD',
                   'SWIFT_AUTH_VERSION', 'SWIFT_ENDPOINT_TYPE', 'SWIFT_REGION', 'SWIFT_DOMAIN_NAME', 'SWIFT_DOMAIN_ID',
                   'SWIFT_PROJECT_NAME', 'SWIFT_PROJECT_ID', 'SWIFT_PROJECT_DOMAIN_NAME', 'SWIFT_PROJECT_DOMAIN_ID']
    ssh_names = WALG_SSH_NAMES
    walg_names = ['WALG_DELTA_MAX_STEPS', 'WALG_DELTA_ORIGIN', 'WALG_DOWNLOAD_CONCURRENCY',
                  'WALG_UPLOAD_CONCURRENCY', 'WALG_UPLOAD_DISK_CONCURRENCY', 'WALG_DISK_RATE_LIMIT',
                  'WALG_NETWORK_RATE_LIMIT', 'WALG_COMPRESSION_METHOD', 'USE_WALG_BACKUP',
                  'USE_WALG_RESTORE', 'WALG_BACKUP_COMPRESSION_METHOD', 'WALG_BACKUP_FROM_REPLICA',
                  'WALG_SENTINEL_USER_DATA', 'WALG_PREVENT_WAL_OVERWRITE', 'WALG_S3_CA_CERT_FILE',
                  'WALG_LIBSODIUM_KEY', 'WALG_LIBSODIUM_KEY_PATH', 'WALG_LIBSODIUM_KEY_TRANSFORM',
                  'WALG_PGP_KEY', 'WALG_PGP_KEY_PATH', 'WALG_PGP_KEY_PASSPHRASE',
                  'no_proxy', 'http_proxy', 'https_proxy']

    wale = defaultdict(lambda: '')
    for name in ['PGVERSION', 'PGPORT', 'WALE_ENV_DIR', 'SCOPE', 'WAL_BUCKET_SCOPE_PREFIX', 'WAL_BUCKET_SCOPE_SUFFIX',
                 'WAL_S3_BUCKET', 'WAL_GCS_BUCKET', 'WAL_GS_BUCKET', 'WAL_SWIFT_BUCKET', 'BACKUP_NUM_TO_RETAIN',
                 'ENABLE_WAL_PATH_COMPAT'] + s3_names + swift_names + gs_names + walg_names + azure_names + \
            azure_auth_names + ssh_names:
        wale[name] = placeholders.get(prefix + name, '')

    if wale.get('WAL_S3_BUCKET') or wale.get('WALE_S3_PREFIX') or wale.get('WALG_S3_PREFIX'):
        wale_endpoint = wale.pop('WALE_S3_ENDPOINT', None)
        aws_endpoint = wale.pop('AWS_ENDPOINT', None)
        aws_region = wale.pop('AWS_REGION', None)

        # for S3-compatible storage we want to specify WALE_S3_ENDPOINT and AWS_ENDPOINT, but not AWS_REGION
        if aws_endpoint or wale_endpoint:
            convention = 'path'
            if not wale_endpoint:
                wale_endpoint = aws_endpoint.replace('://', '+path://')
            else:
                match = re.match(r'^(\w+)\+(\w+)(://.+)$', wale_endpoint)
                if match:
                    convention = match.group(2)
                else:
                    logging.warning('Invalid WALE_S3_ENDPOINT, the format is protocol+convention://hostname:port, '
                                    'but got %s', wale_endpoint)
                if not aws_endpoint:
                    aws_endpoint = match.expand(r'\1\3') if match else wale_endpoint
            wale.update(WALE_S3_ENDPOINT=wale_endpoint, AWS_ENDPOINT=aws_endpoint)
            for name in ('WALE_DISABLE_S3_SSE', 'WALG_DISABLE_S3_SSE'):
                if not wale.get(name):
                    wale[name] = 'true'
            wale['AWS_S3_FORCE_PATH_STYLE'] = 'true' if convention == 'path' else 'false'
            if aws_region and wale.get('USE_WALG_BACKUP') == 'true':
                wale['AWS_REGION'] = aws_region
        elif not aws_region:
            # try to determine region from the endpoint or bucket name
            name = wale.get('WAL_S3_BUCKET') or wale.get('WALE_S3_PREFIX')
            match = re.search(r'.*(\w{2}-\w+-\d)-.*', name)
            if match:
                aws_region = match.group(1)
            else:
                aws_region = placeholders['instance_data']['zone'][:-1]
            wale['AWS_REGION'] = aws_region
        else:
            wale['AWS_REGION'] = aws_region

        if not (wale.get('AWS_SECRET_ACCESS_KEY') and wale.get('AWS_ACCESS_KEY_ID')):
            wale['AWS_INSTANCE_PROFILE'] = 'true'

        if wale.get('WALE_DISABLE_S3_SSE') and not wale.get('WALG_DISABLE_S3_SSE'):
            wale['WALG_DISABLE_S3_SSE'] = wale['WALE_DISABLE_S3_SSE']

        if wale.get('USE_WALG_BACKUP') and wale.get('WALG_DISABLE_S3_SSE') != 'true' and not wale.get('WALG_S3_SSE'):
            wale['WALG_S3_SSE'] = 'AES256'
        write_envdir_names = s3_names + walg_names
    elif wale.get('WAL_GCS_BUCKET') or wale.get('WAL_GS_BUCKET') or\
            wale.get('WALE_GCS_PREFIX') or wale.get('WALE_GS_PREFIX') or wale.get('WALG_GS_PREFIX'):
        if wale.get('WALE_GCS_PREFIX'):
            wale['WALE_GS_PREFIX'] = wale['WALE_GCS_PREFIX']
        elif wale.get('WAL_GCS_BUCKET'):
            wale['WAL_GS_BUCKET'] = wale['WAL_GCS_BUCKET']
        write_envdir_names = gs_names + walg_names
    elif wale.get('WAL_SWIFT_BUCKET') or wale.get('WALE_SWIFT_PREFIX'):
        write_envdir_names = swift_names
    elif wale.get("WALG_AZ_PREFIX"):
        azure_auth = []
        auth_opts = 0

        if wale.get('AZURE_STORAGE_ACCESS_KEY'):
            azure_auth.append('AZURE_STORAGE_ACCESS_KEY')
            auth_opts += 1

        if wale.get('AZURE_STORAGE_SAS_TOKEN'):
            if auth_opts == 0:
                azure_auth.append('AZURE_STORAGE_SAS_TOKEN')
            auth_opts += 1

        if wale.get('AZURE_CLIENT_ID') and wale.get('AZURE_CLIENT_SECRET') and wale.get('AZURE_TENANT_ID'):
            if auth_opts == 0:
                azure_auth.extend(['AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID'])
            auth_opts += 1

        if auth_opts > 1:
            logging.warning('Multiple authentication options configured for wal-g backup to Azure, using %s. Provide '
                            'either AZURE_STORAGE_ACCESS_KEY or AZURE_STORAGE_SAS_TOKEN or Service Principal '
                            '(AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID) for authentication (or use '
                            'MSI).', '/'.join(azure_auth))

        write_envdir_names = azure_names + azure_auth + walg_names

    elif wale.get("WALG_SSH_PREFIX"):
        write_envdir_names = ssh_names + walg_names
    else:
        return

    prefix_env_name = write_envdir_names[0]
    store_type = prefix_env_name[5:].split('_')[0]
    if not wale.get(prefix_env_name):  # WALE_*_PREFIX is not defined in the environment
        bucket_path = '/spilo/{WAL_BUCKET_SCOPE_PREFIX}{SCOPE}{WAL_BUCKET_SCOPE_SUFFIX}/wal/{PGVERSION}'.format(**wale)
        prefix_template = '{0}://{{WAL_{1}_BUCKET}}{2}'.format(store_type.lower(), store_type, bucket_path)
        wale[prefix_env_name] = prefix_template.format(**wale)
    # Set WALG_*_PREFIX for future compatibility
    if store_type in ('S3', 'GS') and not wale.get(write_envdir_names[1]):
        wale[write_envdir_names[1]] = wale[prefix_env_name]

    if not os.path.exists(wale['WALE_ENV_DIR']):
        os.makedirs(wale['WALE_ENV_DIR'])

    wale['WALE_LOG_DESTINATION'] = 'stderr'
    for name in write_envdir_names + ['WALE_LOG_DESTINATION', 'PGPORT'] + ([] if prefix else ['BACKUP_NUM_TO_RETAIN']):
        if wale.get(name):
            path = os.path.join(wale['WALE_ENV_DIR'], name)
            write_file(wale[name], path, overwrite)
            adjust_owner(path, gid=-1)

    if not os.path.exists(placeholders['WALE_TMPDIR']):
        os.makedirs(placeholders['WALE_TMPDIR'])
        os.chmod(placeholders['WALE_TMPDIR'], 0o1777)

    write_file(placeholders['WALE_TMPDIR'], os.path.join(wale['WALE_ENV_DIR'], 'TMPDIR'), True)


def update_and_write_wale_configuration(placeholders, prefix, overwrite):
    set_walg_placeholders(placeholders, prefix)
    write_wale_environment(placeholders, prefix, overwrite)


def write_clone_pgpass(placeholders, overwrite):
    pgpassfile = placeholders['CLONE_PGPASS']
    # pgpass is host:port:database:user:password
    r = {'host': escape_pgpass_value(placeholders['CLONE_HOST']),
         'port': placeholders['CLONE_PORT'],
         'database': '*',
         'user': escape_pgpass_value(placeholders['CLONE_USER']),
         'password': escape_pgpass_value(placeholders['CLONE_PASSWORD'])}
    pgpass_string = "{host}:{port}:{database}:{user}:{password}".format(**r)
    write_file(pgpass_string, pgpassfile, overwrite)
    os.chmod(pgpassfile, 0o600)
    adjust_owner(pgpassfile, gid=-1)


def check_crontab(user):
    with open(os.devnull, 'w') as devnull:
        cron_exit = subprocess.call(['crontab', '-lu', user], stdout=devnull, stderr=devnull)
        if cron_exit == 0:
            return logging.warning('Cron for %s is already configured. (Use option --force to overwrite cron)', user)
    return True


def setup_crontab(user, lines):
    lines += ['']  # EOF requires empty line for cron
    c = subprocess.Popen(['crontab', '-u', user, '-'], stdin=subprocess.PIPE)
    c.communicate(input='\n'.join(lines).encode())


def setup_runit_cron(placeholders):
    crontabs = os.path.join(placeholders['RW_DIR'], 'cron', 'crontabs')
    if not os.path.exists(crontabs):
        os.makedirs(crontabs)
        os.chmod(crontabs, 0o1730)
        if os.getuid() == 0:
            import grp
            os.chown(crontabs, -1, grp.getgrnam('crontab').gr_gid)

    link_runit_service(placeholders, 'cron')


def write_crontab(placeholders, overwrite):
    lines = ['PATH={PATH}'.format(**placeholders)]
    root_lines = []

    sys_nice_is_set = no_new_privs = None
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('NoNewPrivs:'):
                no_new_privs = bool(int(line[12:]))
            elif line.startswith('CapBnd:'):
                sys_nice = 0x800000
                sys_nice_is_set = int(line[8:], 16) & sys_nice == sys_nice

    if sys_nice_is_set:
        renice = '*/5 * * * * bash /scripts/renice.sh'
        if not no_new_privs:
            lines += [renice]
        elif os.getuid() == 0:
            root_lines = [lines[0], renice]
        else:
            logging.info('Skipping creation of renice cron job due to running as not root '
                         'and with "no-new-privileges:true" (allowPrivilegeEscalation=false on K8s)')
    else:
        logging.info('Skipping creation of renice cron job due to lack of SYS_NICE capability')

    if placeholders.get('SSL_TEST_RELOAD'):
        env = ' '.join('{0}="{1}"'.format(n, placeholders[n]) for n in ('PGDATA', 'SSL_CA_FILE', 'SSL_CRL_FILE',
                       'SSL_CERTIFICATE_FILE', 'SSL_PRIVATE_KEY_FILE') if placeholders.get(n))
        hash_dir = os.path.join(placeholders['RW_DIR'], 'tmp')
        lines += ['*/5 * * * * {0} /scripts/test_reload_ssl.sh {1}'.format(env, hash_dir)]

    if bool(placeholders.get('USE_WALE')):
        lines += [('{BACKUP_SCHEDULE} envdir "{WALE_ENV_DIR}" /scripts/postgres_backup.sh' +
                   ' "{PGDATA}"').format(**placeholders)]

    if bool(placeholders.get('LOG_S3_BUCKET')):
        lines += [('{LOG_SHIP_SCHEDULE} nice -n 5 envdir "{LOG_ENV_DIR}"' +
                   ' /scripts/upload_pg_log_to_s3.py').format(**placeholders)]

    lines += yaml.load(placeholders['CRONTAB'])

    if len(lines) > 1 or root_lines:
        setup_runit_cron(placeholders)

    if len(lines) > 1 and (overwrite or check_crontab('postgres')):
        setup_crontab('postgres', lines)

    if root_lines and (overwrite or check_crontab('root')):
        setup_crontab('root', root_lines)


def write_pam_oauth2_configuration(placeholders, overwrite):
    pam_oauth2_args = placeholders.get('PAM_OAUTH2') or ''
    t = pam_oauth2_args.split()
    if len(t) < 2:
        return logging.info("No PAM_OAUTH2 configuration was specified, skipping")

    r = urlparse(t[0])
    if not r.scheme or r.scheme != 'https':
        return logging.error('First argument of PAM_OAUTH2 must be a valid https url: %s', r)

    pam_oauth2_config = 'auth sufficient pam_oauth2.so {0}\n'.format(pam_oauth2_args)
    pam_oauth2_config += 'account sufficient pam_oauth2.so\n'

    write_file(pam_oauth2_config, '/etc/pam.d/postgresql', overwrite)


def write_pgbouncer_configuration(placeholders, overwrite):
    pgbouncer_config = placeholders.get('PGBOUNCER_CONFIGURATION')
    if not pgbouncer_config:
        return logging.info('No PGBOUNCER_CONFIGURATION was specified, skipping')

    pgbouncer_dir = os.path.join(placeholders['RW_DIR'], 'pgbouncer')
    if not os.path.exists(pgbouncer_dir):
        os.makedirs(pgbouncer_dir)
    write_file(pgbouncer_config, pgbouncer_dir + '/pgbouncer.ini', overwrite)

    pgbouncer_auth = placeholders.get('PGBOUNCER_AUTHENTICATION') or placeholders.get('PGBOUNCER_AUTH')
    if pgbouncer_auth:
        write_file(pgbouncer_auth, pgbouncer_dir + '/userlist.txt', overwrite)

    link_runit_service(placeholders, 'pgbouncer')


def main():
    debug = os.environ.get('DEBUG', '') in ['1', 'true', 'TRUE', 'on', 'ON']
    args = parse_args()

    logging.basicConfig(format='%(asctime)s - bootstrapping - %(levelname)s - %(message)s', level=('DEBUG'
                        if debug else (args.get('loglevel') or 'INFO').upper()))

    provider = get_provider()
    placeholders = get_placeholders(provider)
    logging.info('Looks like you are running %s', provider)

    config = yaml.load(pystache_render(TEMPLATE, placeholders))
    config.update(get_dcs_config(config, placeholders))

    user_config = yaml.load(os.environ.get('SPILO_CONFIGURATION', os.environ.get('PATRONI_CONFIGURATION', ''))) or {}
    if not isinstance(user_config, dict):
        config_var_name = 'SPILO_CONFIGURATION' if 'SPILO_CONFIGURATION' in os.environ else 'PATRONI_CONFIGURATION'
        raise ValueError('{0} should contain a dict, yet it is a {1}'.format(config_var_name, type(user_config)))

    user_config_copy = deepcopy(user_config)
    config = deep_update(user_config_copy, config)

    if provider == PROVIDER_LOCAL and not any(1 for key in config.keys() if key in PATRONI_DCS):
        link_runit_service(placeholders, 'etcd')
        config['etcd'] = {'host': '127.0.0.1:2379'}

    pgdata = config['postgresql']['data_dir']
    version_file = os.path.join(pgdata, 'PG_VERSION')
    # if PG_VERSION file exists stick to it and build respective bin_dir
    if os.path.exists(version_file):
        with open(version_file) as f:
            version = f.read().strip()
            if is_valid_pg_version(version):
                config['postgresql']['bin_dir'] = get_bin_dir(version)

    # try to build bin_dir from PGVERSION if bin_dir is not set in SPILO_CONFIGURATION and PGDATA is empty
    if not config['postgresql'].get('bin_dir'):
        version = os.environ.get('PGVERSION', '')
        if not is_valid_pg_version(version):
            version = get_binary_version('')
        config['postgresql']['bin_dir'] = get_bin_dir(version)

    placeholders['PGVERSION'] = get_binary_version(config['postgresql'].get('bin_dir'))
    version = float(placeholders['PGVERSION'])
    if 'shared_preload_libraries' not in user_config.get('postgresql', {}).get('parameters', {}):
        config['postgresql']['parameters']['shared_preload_libraries'] =\
                append_extensions(config['postgresql']['parameters']['shared_preload_libraries'], version)
    if 'extwlist.extensions' not in user_config.get('postgresql', {}).get('parameters', {}):
        config['postgresql']['parameters']['extwlist.extensions'] =\
                append_extensions(config['postgresql']['parameters']['extwlist.extensions'], version, True)

    # Ensure replication is available
    if 'pg_hba' in config['bootstrap'] and not any(['replication' in i for i in config['bootstrap']['pg_hba']]):
        rep_hba = 'hostssl replication {} all md5'.\
            format(config['postgresql']['authentication']['replication']['username'])
        config['bootstrap']['pg_hba'].insert(0, rep_hba)

    for section in args['sections']:
        logging.info('Configuring %s', section)
        if section == 'patroni':
            write_patroni_config(config, args['force'])
            adjust_owner(PATRONI_CONFIG_FILE, gid=-1)
            link_runit_service(placeholders, 'patroni')
            pg_socket_dir = '/run/postgresql'
            if not os.path.exists(pg_socket_dir):
                os.makedirs(pg_socket_dir)
                os.chmod(pg_socket_dir, 0o2775)
                adjust_owner(pg_socket_dir)
        elif section == 'pgqd':
            link_runit_service(placeholders, 'pgqd')
        elif section == 'log':
            if bool(placeholders.get('LOG_S3_BUCKET')):
                write_log_environment(placeholders)
        elif section == 'wal-e':
            if placeholders['USE_WALE']:
                write_wale_environment(placeholders, '', args['force'])
        elif section == 'certificate':
            write_certificates(placeholders, args['force'])
            write_restapi_certificates(placeholders, args['force'])
        elif section == 'crontab':
            write_crontab(placeholders, args['force'])
        elif section == 'pam-oauth2':
            write_pam_oauth2_configuration(placeholders, args['force'])
        elif section == 'pgbouncer':
            write_pgbouncer_configuration(placeholders, args['force'])
        elif section == 'bootstrap':
            if placeholders['CLONE_WITH_WALE']:
                update_and_write_wale_configuration(placeholders, 'CLONE_', args['force'])
            if placeholders['CLONE_WITH_BASEBACKUP']:
                write_clone_pgpass(placeholders, args['force'])
        elif section == 'standby-cluster':
            if placeholders['STANDBY_WITH_WALE']:
                update_and_write_wale_configuration(placeholders, 'STANDBY_', args['force'])
        else:
            raise Exception('Unknown section: {}'.format(section))

    # We will abuse non zero exit code as an indicator for the launch.sh that it should not even try to create a backup
    sys.exit(int(not placeholders['USE_WALE']))


def escape_pgpass_value(val):
    output = []
    for c in val:
        if c in ('\\', ':'):
            output.append('\\')
        output.append(c)
    return ''.join(output)


if __name__ == '__main__':
    main()
