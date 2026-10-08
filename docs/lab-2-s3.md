# Lab 2 – Object storage with S3: answers

## Why are the credentials stored in a Secret and not in the ConfigMap?

A ConfigMap is readable in plain text by anyone with access to the namespace. A Secret has its own access permissions,
is not displayed by `kubectl describe`, and can be encrypted at rest. Base64 is not encryption.

## The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?

It would fail (`ExpiredToken`) because the Secret contains a temporary token that has already expired. In production,
we use a workload identity: the ServiceAccount is linked to an IAM role (IRSA, Workload Identity) and receives
automatically renewed credentials. We can also use a secret manager such as Vault.

## How would you turn this Job into a daily ingestion?

We convert the Job into a CronJob (`schedule: "0 2 * * *"`, `concurrencyPolicy: Forbid`). We read the data from the
source rather than from a ConfigMap, and we partition the files by date (`bronze/orders/date=…/`) to avoid overwriting
the history.
