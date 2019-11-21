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


def create(managers, blueprint_id, deployment_id, inputs_file, outputs_file, **kwargs):
    manager_id = managers['topologies'][blueprint_id]
    client = _get_rest_client(managers, manager_id)
    with open(inputs_file, 'r') as f:
        inputs = yaml.safe_load(f)
    client.deployments.create(
        blueprint_id=blueprint_id,
        deployment_id=deployment_id,
        inputs=inputs)
    # Wait for deployment to finish (needed as per CYBL-955).
    dep_execution = client.executions.list(deployment_id=deployment_id)[0]
    follow_execution(client, dep_execution)
    install_execution = client.executions.start(
        deployment_id=deployment_id,
        workflow_id='install'
    )
    follow_execution(client, install_execution)
    capabilities = client.deployments.capabilities.get(deployment_id)
    outputs = client.deployments.outputs.get(deployment_id)
    with open(outputs_file, 'w') as f:
        json.dump({
            'manager_id': manager_id,
            'deployment_id': deployment_id,
            'outputs': outputs.outputs,
            'capabilities': capabilities.capabilities
        }, f, indent=4)


def delete(managers, manager_id, deployment_id, **kwargs):
    client = _get_rest_client(managers, manager_id)
    uninstall_execution = client.executions.start(
        deployment_id=deployment_id,
        workflow_id='uninstall'
    )
    uninstall_execution = follow_execution(client, uninstall_execution)
    follow_execution(client, uninstall_execution)
    client.deployments.delete(deployment_id=deployment_id)


def main():
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('--id', dest='deployment_id', metavar='ID', required=True)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    create_subparser = subparsers.add_parser('create', parents=[common_parser])
    create_subparser.add_argument('-b', '--blueprint', dest='blueprint_id', metavar='ID', required=True)
    create_subparser.add_argument('-i', '--inputs', dest='inputs_file', metavar='FILE', required=True)
    create_subparser.add_argument('-o', '--outputs', dest='outputs_file', metavar='FILE', required=True)
    create_subparser.set_defaults(func=create)
    delete_subparser = subparsers.add_parser('delete', parents=[common_parser])
    delete_subparser.add_argument('--manager-id', metavar='ID', required=True)
    delete_subparser.set_defaults(func=delete)

    with open(os.path.join(THIS_DIR, 'managers.yaml')) as f:
        managers = yaml.safe_load(f)

    args = parser.parse_args()
    var_args = vars(args)
    args.func(managers, **var_args)


if __name__ == "__main__":
    main()

