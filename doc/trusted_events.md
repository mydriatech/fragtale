# Level of trust in delivered events

Events delivered by or retrieved from Fragtale can be trusted to:

* be identical to the received published event
* have been received at a certain time (with configurable tolerance)

An event can hence be known with to have happened before a point in time.
With reasonable assumptions and knowledge about the overall system and the
publishing microservice, a time interval for when an event has happened could be
deducted.

## Implementation overview

Design goals:

* Data at rest should be tamper evident to allow use of an external specialized
  storage provider that can be trusted to keep event sufficiently confidential.
* Low overhead, quick validation and highly scalable solution.
* Secrets should not be exposed to operation teams using Gitops.
* No additional dependency on third party services except (optional) NTP that
  could cause a denial of service.

Securing connections to and from the Fragtale API is outside the scope of this
document. (Hint: use TLS)

## Accuracy of when an event was published

The local system time is monitored for correctness compared to an NTP server.
Events will be rejected if the current system time deviates outside the
configured tolerance.


## Integrity

A message digest (hash) is created over the event document and the time it was
received.

The message digest is included in a binary digest tree (BDT) together with other
messages that are published to a topic at approximately the same time.
The root of the BDT is protected with dual protection algorithms.
A proof of inclusion in the BDT is added to the event.

Roots of the first level BDTs are included into a second BDT with larger time
span and the root is protected with dual protection algorithms.
A proof of inclusion in the second level BDT is added to each first level BDT.

Roots of the second level BDTs are included into a third BDT with even larger
time span and the root is protected with dual protection algorithms.
A proof of inclusion in the third level BDT is added to each second level BDT.


## Secrets and roll-over

The dual protection algorithms use a new and an old secret (to allow roll-over
and protection algorithm changes).

The secrets are generated during application deployment and stored in Kubernetes
Secret objects.
If you are using Gitops this ensures that the secrets are not available to the
one installing the application.

Roll-over can be triggered by increasing a counter configuration and
re-deploying the application once the application log of the oldest node states
that roll-over is allowed.


## Additional details

Please see the Helm chart default configuration file and code's documentation
for further explanations about lookup efficiency and time intervals.
