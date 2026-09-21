
## Black box tests execution
To run the black box tests with using Docker, the local Docker images of JnksIOT's microservices should be built. <br />
- Build the local Docker images in the directory with the JnksIOT's main [pom.xml](./../../pom.xml):
        
        mvn clean install -Ddockerfile.skip=false
- Verify that the new local images were built: 

        docker image ls
As result, in REPOSITORY column, next images should be present:
        
        jnks-iot/jnks-iot-coap-transport
        jnks-iot/jnks-iot-lwm2m-transport
        jnks-iot/jnks-iot-http-transport
        jnks-iot/jnks-iot-mqtt-transport
        jnks-iot/jnks-iot-snmp-transport
        jnks-iot/jnks-iot-core
        jnks-iot/jnks-iot-rule-engine
        jnks-iot/jnks-iot-web-ui
        jnks-iot/jnks-iot-js-executor

- Run the black box tests (without ui tests) in the [tests/black-box-tests](../black-box-tests) directory with Redis standalone:

        mvn clean install -DblackBoxTests.skip=false

- Run the black box tests (without ui tests) in the [tests/black-box-tests](../black-box-tests) directory with Redis standalone with TLS:

        mvn clean install -DblackBoxTests.skip=false -DblackBoxTests.redisSsl=true

- Run the black box tests in the [tests/black-box-tests](../black-box-tests) directory with Redis cluster:

        mvn clean install -DblackBoxTests.skip=false -DblackBoxTests.redisCluster=true

- Run the black box tests in the [tests/black-box-tests](../black-box-tests) directory with Redis sentinel:

        mvn clean install -DblackBoxTests.skip=false -DblackBoxTests.redisSentinel=true

- Run the black box tests in the [tests/black-box-tests](../black-box-tests) directory in Hybrid mode (postgres + cassandra):

        mvn clean install -DblackBoxTests.skip=false -DblackBoxTests.hybridMode=true

- To run the black box tests with using local env run tests in the [tests/black-box-tests](../black-box-tests) directory with runLocal property:

        mvn clean install -DblackBoxTests.skip=false -DrunLocal=true

- To run only ui tests in the [tests/black-box-tests](../black-box-tests) directory: 

        mvn clean install -DblackBoxTests.skip=false -Dsuite=uiTests

- To run only ui smoke rule chains tests in the [tests/black-box-tests](../black-box-tests) directory:

        mvn clean install -DblackBoxTests.skip=false -Dsuite=smokesRuleChain

- To run only ui smoke customers tests in the [tests/black-box-tests](../black-box-tests) directory:

        mvn clean install -DblackBoxTests.skip=false -Dsuite=smokesCustomer

- To run only ui smoke profiles tests in the [tests/black-box-tests](../black-box-tests) directory:

        mvn clean install -DblackBoxTests.skip=false -Dsuite=smokesPrifiles

- To run all tests (black-box and ui) in the [tests/black-box-tests](../black-box-tests) directory:

        mvn clean install -DblackBoxTests.skip=false -Dsuite=all 

### To run a separate test manually on a built UI:
1. Add the black-box-tests module in the [pom.xml](./../../pom.xml) or add as a Maven project
2. Add Vm Option "*-DrunLocal=true -Dtb.baseUiUrl=http://localhost:4200/*" in "Run" -> "Edit Configuration" -> "Edit Configuration Templates" -> "TestNG"
3. To run a specific test, go to the test class in the [UI tests package](src/test/java/org/jnks-iot/server/msa/ui/tests) and run the test. Alternatively, go to the [resources](src/test/resources) in the black-box-tests module and run the test suite that you need.