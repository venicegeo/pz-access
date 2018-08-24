#pz-access
The pz-access project provides REST services that are responsible for handling access to data - either by requesting metadata, requesting file downloads, or requesting GeoServer deployments of data. When requesting GeoServer deployments of loaded data into Piazza, the Access component will transfer the appropriate files over to the GeoServer data directory, and then create a deployment lease that provides a guarantee for a certain length of time that the data will be available on the Piazza GeoServer instance.

## Requirements
Before building and running the pz-access project, please ensure that the following components are available and/or installed:
- [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (JDK for building/developing, otherwise JRE is fine)
- [Maven (v3 or later)](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for checking out repository source)
- [Eclipse](https://www.eclipse.org/downloads/), or any maven-supported IDE
- [RabbitMQ](https://www.rabbitmq.com/download.html)
- [PostgreSQL](https://www.postgresql.org/download)
- [GeoServer](http://docs.geoserver.org/stable/en/user/installation/index.html)
- Access to Nexus is required to build

Ensure that the nexus url environment variable `ARTIFACT_STORAGE_URL` is set:

	$ export ARTIFACT_STORAGE_URL={Artifact Storage URL}

For additional details on prerequisites, please refer to the Piazza Developer's Guide repository content for [Core Overview](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/02-pz-core.md) or [Piazza Access](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/09-pz-access.md) sections. Also refer to the [prerequisites for using Piazza](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/03-jobs.md) section for additional details.

## Setup, Configuring & Running

### Setup
Create the directory the repository must live in, and clone the git repository:

    $ mkdir -p {PROJECT_DIR}/src/github.com/venicegeo
	$ cd {PROJECT_DIR}/src/github.com/venicegeo
    $ git clone git@github.com:venicegeo/pz-access.git
    $ cd pz-access

>__Note:__ In the above commands, replace {PROJECT_DIR} with the local directory path for where the project source is to be installed.

### Configuring
As noted in the Requirements section, to build and run this project, RabbitMQ, PostgreSQL, and GeoServer are required. The `application.properties` file controls URL information for these components it connects to - mainly GeoServer. If you are intending to debug a local instance of GeoServer, then simply changing the appropriate vcap.services.pz-geoserver.* configuration values.

### Running
To build and run the Access service locally, pz-access can be run using Eclipse any maven-supported IDE. Alternatively, pz-access can be run through command line interface (CLI), by navigating to the project directory and run:

`mvn clean install -U spring-boot:run`

> __Note:__ This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail.
