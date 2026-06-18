# TiDB2DW Micro-Service

## Goals

- TiDB2DW supports multiple replication streams for multiple upstream TiDB clusters.
- TiDB2DW has high availability, can tolerante single node failure.

## Design

### TiDB2DW Owner

The Owner maintains replication tasks meta information, and monitor the health of workers. There is only one active owner at any point of time, if the active owner failed, the standby owner will be the new active owner.

### TiDB2DW Worker

The workers handle replication tasks, they will send heartbeat to the active owner periodically. If one worker is failed or isolated with the active owner, tasks on this workers will be scheduled to other workers.