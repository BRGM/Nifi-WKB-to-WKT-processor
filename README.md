# Nifi-WKT-to-WKB-processor
A simple processor to transform WKB field in flowfile content to WKT in flowfile attribute.

Many thanks to Phillip Grenier for example : 
http://www.nifi.rocks/developing-a-custom-apache-nifi-processor-json/
https://github.com/pcgrenier/nifi-examples



## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Getting Help](#getting-help)
- [License](#license)


## Features

Flowfile in apache Nifi can contain WKB (well known binary) field in the payload. This is, for example, the output provided from debezium when you capture event from postgis table with column in geography/geometry type.  Most products, as solr with jts, are fulfilled using WKT (well known text) to represent a geography/geometry type.

This processor help us to convert wkb to wkb in order to integrate data to other systems.

## Requirements

Apache nifi 1.5.0 only tested using apache nifi 1.5.0.
Maven 3+
Java JDK 8+.

## Getting Started

To build:
- Execute `mvn clean install` on the root of project (run the parent pom). After an amount of output you should eventually see a success message.

        laptop:nifi myuser$ mvn clean install
        [INFO] ------------------------------------------------------------------------
        [INFO] Reactor Summary:
        [INFO]
        [INFO] nifi-geoformatconvertor-bundle ..................... SUCCESS [  9.796 s]
        [INFO] nifi-geoformatconvertor-processors ................. SUCCESS [  9.259 s]
        [INFO] nifi-geoformatconvertor-nar ........................ SUCCESS [  0.985 s]
        [INFO] ------------------------------------------------------------------------
        [INFO] BUILD SUCCESS
        [INFO] ------------------------------------------------------------------------
        [INFO] Total time: 30.663 s
        [INFO] Finished at: 2018-03-15T15:16:25+01:00
        [INFO] Final Memory: 29M/488M
        [INFO] ------------------------------------------------------------------------


- The *.nar file is the archive to put into nifi lib directory. The archive is located into nifi-geoformatconvertor-nar/target directory.

## Getting Help

Processor will provide converted value to flowfile attributes with the name provided from properties.

You have to specify the jsonpath where to find the wkb value, and provide the name of the wkt attribute to return the value.

As usual with nifi processor, there are two relationships, SUCCESS and FAILED, as success is used when conversion was done correctly, and failed when conversion raised a error. The problem is documented in flowfile attribute, with in name the processor classname "WKBWKTConvertProcessor" and in value the exception message.

About nifi custom processor installation, please refer to nifi documentation.
https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#processor_api

## License

This project is under MIT licence.

Please refer to Licence file at the root of this repository for details.