#!/bin/bash
start-db.sh

CONF_FOLDER="${pkg.installFolder}/conf"
jarfile=${pkg.installFolder}/bin/${pkg.name}.jar
configfile=${pkg.name}.conf

source "${CONF_FOLDER}/${configfile}"

echo "Starting JnksIOT upgrade ..."

java -cp ${jarfile} $JAVA_OPTS -Dloader.main=com.jnks.iot.server.JnksIotInstallApplication \
                -Dspring.jpa.hibernate.ddl-auto=none \
                -Dinstall.upgrade=true \
                -Dlogging.config=/usr/share/jnks-iot/bin/install/logback.xml \
                org.springframework.boot.loader.launch.PropertiesLauncher

stop-db.sh