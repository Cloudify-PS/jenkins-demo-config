#!/usr/bin/env python

import argparse
import yaml
import json
import logging
import os
import time
import sys

from cloudify_rest_client.client import CloudifyClient
from cloudify_rest_client.executions import Execution

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

THIS_DIR = os.path.dirname(__file__)


def follow_execution(client, execution):
    logger.info(
        "Beginning to follow execution: id=%s, workflow=%s%s",
        execution.id,
        execution.workflow_id,
        ", deployment=%s" % execution.deployment_id if execution.deployment_id else "")
    offset = 0
    execution_ended = False
    while True:
        events_list_response = client.events.list(
            execution_id=execution.id,
            _offset=offset,
            _size=500,
            include_logs=True,
            sort='reported_timestamp'
        )
        for item in events_list_response.items:
            logger.log(
                getattr(logging, item.get('level', 'info').upper()),
                "[%s] [%s] %s%s",
                item['reported_timestamp'],
                execution.deployment_id,
                "[%s] " % item['node_instance_id'] if item.get('node_instance_id', None) else '',
                item['message'])

        offset += len(events_list_response.items)
        # If more events may be available - get them.
        if offset < events_list_response.metadata.pagination.total:
            continue
        # If it already ended in the previous iteration, time to leave.
        if execution_ended:
            break
        # Check the status of the execution.
        execution = client.executions.get(execution.id, _include=['id', 'status', 'deployment_id', 'workflow_id'])
        # If the execution status is still 'started', then we should continue for sure, after waiting for a second
        # (to avoid unnecessary spins). If it's in an end state, then repeat the loop without wait.
        if execution.status in Execution.END_STATES:
            execution_ended = True
        else:
            time.sleep(1)

    if execution_ended:
        logger.info("Finished following execution of '%s' for deployment '%s'", execution.workflow_id,
                    execution.deployment_id)
    else:
        logger.warning("Timed out following execution of '%s' for deployment '%s' to finish", execution.workflow_id, execution.deployment_id)

    if execution.status != Execution.TERMINATED:
        raise Exception("Execution '%s' didn't end properly (status: %s)" % (execution.id, execution.status))

    return execution


def _get_rest_client(managers, manager_id):
    manager_desc = managers['managers'][manager_id]
    return CloudifyClient(**manager_desc)


def _create_deployment(client, blueprint_id, deployment_id, inputs):
    client.deployments.create(
        blueprint_id=blueprint_id,
        deployment_id=deployment_id,
        inputs=inputs)
    # Wait for deployment to finish (CYBL-955 would save this REST call).
    dep_execution = client.executions.list(deployment_id=deployment_id)[0]
    follow_execution(client, dep_execution)


def _delete_deployment(client, deployment_id):
    client.deployments.delete(deployment_id=deployment_id)


def _install(client, deployment_id):
    install_execution = client.executions.start(
        deployment_id=deployment_id,
        workflow_id='install'
    )
    follow_execution(client, install_execution)


def _uninstall(client, deployment_id):
    uninstall_execution = client.executions.start(
        deployment_id=deployment_id,
        workflow_id='uninstall'
    )
    follow_execution(client, uninstall_execution)


def create(managers, blueprint_id, env_deployment_id, inputs_file, outputs_file, **kwargs):
    manager_id = managers['topologies'][blueprint_id]
    client = _get_rest_client(managers, manager_id)
    with open(inputs_file, 'r') as f:
        inputs = yaml.safe_load(f)
    _create_deployment(client, blueprint_id, env_deployment_id, inputs)
    _install(client, env_deployment_id)
    capabilities = client.deployments.capabilities.get(env_deployment_id)
    outputs = client.deployments.outputs.get(env_deployment_id)
    with open(outputs_file, 'w') as f:
        json.dump({
            'manager_id': manager_id,
            'deployment_id': env_deployment_id,
            'outputs': outputs.outputs,
            'capabilities': capabilities.capabilities
        }, f, indent=4)


def delete(managers, manager_id, env_deployment_id, **kwargs):
    client = _get_rest_client(managers, manager_id)
    _uninstall(client, env_deployment_id)
    _delete_deployment(client, env_deployment_id)


def install(managers, manager_id, app_blueprint_path, app_id, inputs_file, **kwargs):
    client = _get_rest_client(managers, manager_id)
    client.blueprints.upload(
        path=app_blueprint_path,
        entity_id=app_id
    )
    with open(inputs_file, 'r') as f:
        inputs = json.load(f)
    _create_deployment(client, app_id, app_id, inputs)
    _install(client, app_id)


def uninstall(managers, manager_id, app_id, **kwargs):
    client = _get_rest_client(managers, manager_id)
    _uninstall(client, app_id)
    _delete_deployment(client, app_id)
    client.blueprints.delete(app_id)


def main():
    common_env_parser = argparse.ArgumentParser(add_help=False)
    common_env_parser.add_argument('--id', dest='env_deployment_id', metavar='ID', required=True)

    common_app_parser = argparse.ArgumentParser(add_help=False)
    common_app_parser.add_argument('--id', dest='app_id', metavar='ID', required=True)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    create_subparser = subparsers.add_parser('create', parents=[common_env_parser])
    create_subparser.add_argument('-b', '--blueprint', dest='blueprint_id', metavar='ID', required=True)
    create_subparser.add_argument('-i', '--inputs', dest='inputs_file', metavar='FILE', required=True)
    create_subparser.add_argument('-o', '--outputs', dest='outputs_file', metavar='FILE', required=True)
    create_subparser.set_defaults(func=create)
    delete_subparser = subparsers.add_parser('delete', parents=[common_env_parser])
    delete_subparser.add_argument('--manager-id', metavar='ID', required=True)
    delete_subparser.set_defaults(func=delete)
    install_subparser = subparsers.add_parser('install', parents=[common_app_parser])
    install_subparser.add_argument('--app-blueprint', dest='app_blueprint_path', metavar='FILE', required=True)
    install_subparser.add_argument('-i', '--inputs', dest='inputs_file', metavar='FILE', required=True)
    install_subparser.add_argument('--manager-id', metavar='ID', required=True)
    install_subparser.set_defaults(func=install)
    uninstall_subparser = subparsers.add_parser('uninstall', parents=[common_app_parser])
    uninstall_subparser.add_argument('--manager-id', metavar='ID', required=True)
    uninstall_subparser.set_defaults(func=uninstall)

    with open(os.path.join(THIS_DIR, 'managers.yaml')) as f:
        managers = yaml.safe_load(f)

    args = parser.parse_args()
    var_args = vars(args)
    args.func(managers, **var_args)


if __name__ == "__main__":
    main()

