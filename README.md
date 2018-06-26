# dist-ds-store

## Choosing pull vs push

Replication from master to slave should be either pull or push. We cannot have both. Replication happens in two scenarios.

1) Normal opertions - when slave reads from master immediately after every write

2) Fail Recover of slave - when a slave recovers after failure, it needs to get all the old writes before it can accept new writes

To ensure proper ordering and consistency, we have to either use pull or push for both. Using push for the first and
pull for the second could result in data loss.

Scenario

Master accepts writes, say at transaction replicaId 1000

Slave pulls transactions starting from 1 until 1000. 

Then it stops pulling and starts accepting write from master. In the elapsed time between stopping to pull and starting to accept from master, there could be message loss

Master - 1 2 3 ..1000 1001 1002 1003 1004 1005

Slave - Pull 1....1000 ....starting up.... 1005

Writes 1001 thru 1004 are lost

Current implementation has this problem