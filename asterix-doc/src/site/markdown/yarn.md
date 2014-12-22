#Introduction#

##<a id="toc">Table of Contents</a> ##
* [Architecture Overview](#arch)
* [Prerequisites](#prereq)
* [Tutorial Installation](#tut)
* [FAQ and Common Issues](#faq)
* [Reference guide to AsterixDB's YARN Client](#detail)

This is a guide describing how to deploy AsterixDB onto a YARN-based environment.


##<a id="arch">Architecture Overview</a>##

AsterixDB uses a shared-nothing architecture and local file-based storage- not HDFS. Hence we are reliant on the local storage on each node. 
This is somewhat different than other applications that run on YARN. This is the main reason why in the configuration file you must select a set of nodes to store your data on. However once an instance is started, it will remember the set of nodes it was started on each time it is started up again and will persist data on these nodes.

AsterixDB is somewhat unusual among YARN-enabled applications in that it stores persistient state on a fixed set of nodes, outside of the life of a single container instance. Therefore keep in mind that the number of nodes chosen for an instance cannot be changed at a later date, and that data is not currently replicated or stored redundantly within AsterixDB.

##<a id="prereq">Prerequisites</a>##
For this tutorial it will be assumed that we have a YARN cluster with the proper environment variables set. To test this, try running the DistributedShell example that is distributed as part of Apache Hadoop. If that sample application can be run successfully then the environment should be acceptable for launching AsterixDB on to your YARN-enabled cluster.

#<a id="tut">Tutorial installation</a>#

##Configuration ##

To deploy AsterixDB onto a YARN cluster, we need to construct a configuration file that describes the resources that will be requested from YARN for AsterixDB. 

<div class="source">
<pre>
<img src="images/yarn_clust.png" alt="Illustration of a simple YARN cluster with AsterixDB processes."/>
<em>Fig. 1</em>:  Illustration of a simple YARN cluster with AsterixDB processes and their locations
</pre>
</div>

This AsterixDB cluster description file corresponds to the above deployed scenario.

        <cluster xmlns="yarn_cluster">
            <name>localtest</name>
            <txn_log_dir>/home/vagrant/</txn_log_dir>
            <iodevices>/home/vagrant/</iodevices>
            <store>asterix-data</store>
            <master_node>
                <id>cc</id>
                <client_ip>10.10.0.2</client_ip>
                <cluster_ip>10.10.0.2</cluster_ip>
                <client_port>1098</client_port>
                <cluster_port>1099</cluster_port>
                <http_port>8888</http_port>
            </master_node>
            <node>
                <id>nc1</id>
                <cluster_ip>10.10.0.2</cluster_ip>
            </node>
            <node>
                <id>nc2</id>
                <cluster_ip>10.10.0.3</cluster_ip>
            </node>
            <node>
                <id>nc3</id>
                <cluster_ip>10.10.0.4</cluster_ip>
            </node>
            <metadata_node>nc1</metadata_node>
        </cluster>

In this example we have 3 NCs and one CC. Each node is defined by a unique name (not necessarily hostname) and an IP on which AsterixDB nodes will listen and communicate with eachother. This is the 'cluster_ip' parameter. The 'client_ip' parameter is the interface on which client-facing services are presented, for example the web interface.

With this configuration in hand, the YARN client can be used to deploy AsterixDB onto the cluster:

        [vagrant@cc asterix-yarn]$ bin/asterix -n my_awesome_instance -c /vagrant/my_awesome_cluster_desc.xml install
        Waiting for new AsterixDB Instance to start  .
        Asterix successfully deployed and is now running.

The instance will be visible in the YARN RM similar to the below image
<div class="source">
<pre>
<img src="images/running_inst.png" alt="Illustration of a simple YARN cluster with AsterixDB processes."/>
<em>Fig. 2</em>:  Hadoop YARN Resource Manager dashboard with running AsterixDB instance
</pre>
</div>

Once the client returns success, the instance is now ready to be used. We can now use the asterix instance at the CC's IP (10.10.0.2), on the default port (19001).


<div class="source">
<pre>
<img src="images/asterix_webui.png" alt="Illustration of a simple YARN cluster with AsterixDB processes." />
<i>Fig. 3</i>:  AsterixDB Web User Interface
</pre>
</div>

To stop the instance that was just deployed, the `stop` command is used:

        [vagrant@cc asterix-yarn]$ bin/asterix -n my_awesome_instance stop
        Stopping instance my_awesome_instance

This attempts a graceful shutdown of the instance. If for some reason this does not succeed, the `kill` action can be used to force shutdown in a similar fashion:

        [vagrant@cc asterix-yarn]$ bin/asterix -n my_awesome_instance kill
        Are you sure you want to kill this instance? In-progress tasks will be aborted
        Are you sure you want to do this? (yes/no): yes

After stopping the instance no containers on any YARN NodeManagers are allocated. However, the state of the instance is still persisted on HDFS and on the local disks of each machine where a Node Controller was deployed. Every instance, running or not can be viewed via the `describe` action:

        [vagrant@cc asterix-yarn]$ bin/asterix describe
        Existing AsterixDB instances:
        Instance my_awesome_instance is stopped

To start the instance back up once more, the `start` action is used:

        [vagrant@cc asterix-yarn]$ bin/asterix -n my_awesome_instance start
        Waiting for AsterixDB instance to resume .
        Asterix successfully deployed and is now running.

#<a id="faq">Frequently Asked Questions and Common Issues</a>#

#<a id="detail">Listing of Commands and Options</a>#

##Overview##

All commands take the format

        asterix [action-specific option] [action]

###Technical details###

AsterixDB's YARN client is based on static allocation of containers within Node Managers based on IP. The AM and CC processes are currently not integrated in any fashion

##Actions##

Below is a description of the various actions available via the AsterixDB YARN client

| Action     | Description                                                                                                                   |
|------------|-------------------------------------------------------------------------------------------------------------------------------|
| `start`    | Starts an existing instance specified by the -name flag                                                                       |
| `install`  | Deploys and starts an AsterixDB instance described by the config specified in the -c parameter, and named by the -n parameter |
| `stop`     | Attempts graceful shutdown of an AsterixDB instance specified in the -name parameter                                          |
| `kill`     | Forcefully stops an instance by asking YARN to terminate all of its containers.                                               |
| `destroy`  | Remove the instance specified by -name and all of its stored resources from the cluster                                       |
| `describe` | Show all instances, running or not, visible to the AsterixDB YARN client                                                      |

##Options##
Below are all availabe options, and which actions they can be applied to

| Option                      | Long Form       | Short Form | Usage                                                                                        | Applicability                                                                                                                                                                                                                                                                                       |
|-----------------------------|-----------------|------------|----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Configuration Path          | `-asterixConf`  | `-c`       | ` -c [/path/to/file]`. Path to an AsterixDB Cluster Description File                         | Only required with `create` . A configuration in DFS defines the existance of an instance.                                                                                                                                                                                                          |
| Instance Name               | `-name`         | `-n`       | `-n [instance name]` Name/Identifier for instance.                                           | Required for all actions except `describe` and `lsbackup`                                                                                                                                                                                                                                           |
| Asterix Binary Path         | `-asterixTar`   | `-tar`     | `-tar [/path/to/binary]` Path to asterix-server binary.                                      | This is the AsterixDB server binary that is distributed and run on the DFS. Usually set by default via the launcher script and cached for each instance. Can be manually set, only used in `create` and `install` with `-r`                                                                         |
| Force                       | `-force`        | `-f`       | `-f`. Use at your own risk. Disables any sanity-checking during an action.                   | Can be applied to any action, but is mostly useful in cases where an instance cannot be removed properly via `destroy` and cleanup of DFS files is desired.                                                                                                                                         |
| Refresh                     | `-refresh`      | `-r`        | `-r`. Replaces cached binary with one mentioned in `-tar`.                                   | This only has an effect with the `start` action. It can be used to replace/upgrade the binary cached for an instance on the DFS.                                                                                                                                                                    |
| Base Parameters             | `-baseConf`     | `-bc`      | `-bc [path/to/params]`. Specifies parameter file to use during instance creation/alteration. | This file specifies various internal properties of the AsterixDB system, such as Buffer Cache size and Page size, among many others. It can be helpful to tweak parameters in this file, however caution should be exercised in keeping them at sane values. Only used during `alter` and `create`. |
| External library path       | `-externalLibs` | `-l`       | `-l [path/to/library]`. Specifies an external library to upload to an existing instance.      | Only used in `libinstall`. Specifies the file containing the external function to install                                                                                                                                                                                                           |
| External library dataverse. | `-libDataverse` | `-ld`      | `-ld [existing dataverse name]`                                                              | Only used in `libinstall`. Specifies the dataverse to install the library in an `-l` option to.                                                                                                                                                                                                     |
| Snapshot ID                 | `-snapshot`     | [none]     | `-snapshot [backup timestamp/ID]`                                                            | Used with `rmbackup` and `restore` to specify which backup to perform the respective operation on.                                                                                                                                                                                                  |
