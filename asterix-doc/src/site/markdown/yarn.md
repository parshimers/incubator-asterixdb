# Introduction #

##<a id="toc">Table of Contents</a> ##

This is a guide describing how to deploy AsterixDB onto a YARN-based environment. 


# Architecture overview #

AsterixDB uses a shared-nothing architecture and local file-based storage- not HDFS. Hence we are reliant on the local storage on each node. 
This is somewhat different than other applications that run on YARN. This is the main reason why in the configuration file you must select a set of nodes to store your data on. However once an instance is started, it will remember the set of nodes it was started on each time it is started up again and will persist data on these nodes.




# Prerequisites # 
For this tutorial it will be assumed that we have a YARN cluster with the proper environment variables set. To test this, try running the DistributedShell example that is distributed as part of Apache Hadoop. If it can be run successfully then the environment should be acceptable for launching AsterixDB on to your YARN cluster.

AsterixDB is somewhat unusual among YARN-enabled applications in that it stores persistient state on a fixed set of nodes. Therefore keep in mind that the number of nodes chosen for an instance cannot be changed at a later date, and that data is not currently replicated or stored redundantly within AsterixDB.
 
# Configuration #

To deploy AsterixDB onto a YARN cluster, we need to construct a configuration file that describes the resources that will be requested from YARN for AsterixDB. An example is below 

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

        asterix -n my_instance -c /path/to/config.xml install

Once the client returns success, the instance is now ready to be used. 

To stop the instance that was just deployed, the 'stop' command is used:

        asterix -n my_instance stop

This attempts a graceful shutdown of the instance. If for some reason this does not succeed, the 'kill' command can be used to force shutdown in a similar fashion:

        asterix -n my_instance kill


There are some activites now that can be performed on the instance while it is inactive but still deployed. One action can be to install an external library like so:


After stopping the instance no containers on any YARN NodeManagers are allocated. However, the state of the instance is still persisted on HDFS and on the local disks of each machine where a Node Controller was deployed. To start the instance back up once more, the start command is used:

       asterix -n my_instance start




<table>
<tr>
<td>Command</td>
<td>Description</td>
</tr>

<tr>
<td>start</td>
<td>Starts an existing instance specified by the -name flag</td>
</tr>

<tr>
<td>install</td>
<td>Deploys and starts an AsterixDB instance described by the config specified in the -c parameter, and named by the -n parameter</td>
</tr>

<tr>
<td>stop</td>
<td>Attempts graceful shutdown of an AsterixDB instance specified in the -name parameter</td>
</tr>

<tr>
<td>kill</td>
<td>Forcefully stops an instance by asking YARN to terminate all of its containers.</td>
</tr>

<tr>
<td>destroy</td>
<td>Remove the instance specified by -name and all of its stored resources from the cluster</td>
</tr>

<tr>
<td>describe</td>
<td>Show all instances, running or not, visible to the AsterixDB YARN client</td>
</tr>
</table>
