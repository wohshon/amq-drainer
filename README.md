## AMQ message drainer / migration

A sample program to create an Embedded AMQ instance to drain off orphaned messages.

This is applicable in usecases where we need to minimize downtime while shutting down AMQ instances, due to upgrade or patching work.

The embedded instance points to the message store  (using a broker.xml file) of the AMQ instance and reads messages off the queues, forwards the messages to another (newly setup / patched) instance, usually in a blue green kind of a setup.
Once the messages in the source instance is emptied, the instance can be shutdown.

In cases where you have visibility and control over all your producers and clients, this drainer may not be needed. You can point your producers and clients in the right sequence to a new AMQ instance to achieve zero downtime cutover. You can also explore using the the data tools that comes with Artemis to export and import data if you are able to afford downtime to your clusters

The drainer will be useful for uses cases where inlight messages could be 'stuck' in the original instance, or not all your producers / consumers can be swing over in time / or controlled by you.

So as a generic sequence, to cutover from an existing AMQ instance

- setup a new AMQ7 / Artemis cluster 
- Swing over producers, followed by consumers to new cluster
- Run the drainer with the following command:

Do take note of the congifuration in broker.xml where it points to a journal store of the source instance.

There is a remote AMQconnectionFactory that is setup to point to a desination AMQ instance to forward the messages to

Please adjust them accordingly

    mvn exec:java -Dexec.mainClass="com.redhat.demo.drainer.DrainerAMQ" -D mode=DRAIN -q


This sample code is not Production ready.