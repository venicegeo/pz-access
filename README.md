To run the Access service locally, perhaps through Eclipse or through CLI, navigate to the project directory and run:

mvn clean install -U spring-boot:run

The application.properties file controls URL information for the components it connects to - mainly GeoServer. If you are intending to debug a local instance of GeoServer, then simply changing the appropriate vcap.services.pz-geoserver.* configuration values.
