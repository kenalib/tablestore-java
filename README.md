
# Table Store Java

Sample code of Table Store Java SDK


## Account setup

* this setup is to replace id in `src/main/resources/account.properties`
* add below in `pom.xml`

```xml
<project>
  <properties>
    <keyId>${keyId}</keyId>
    <keySecret>${keySecret}</keySecret>
  </properties>
  <build>
    <resources>
      <resource>
        <directory>src/main/resources</directory>
        <filtering>true</filtering>
      </resource>
    </resources>
  </build>
</project>
```

* create `~/.m2/settings.xml` as below with replacing to your real id.

```xml
<settings xmlns="http://maven.apache .org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>table-store-profile</id>
        <activation>
          <activeByDefault>true</activeByDefault>
        </activation>
        <properties>
          <keyId>YOUR_KEY_ID</keyId>
          <keySecret>YOUR_KEY_SECRET</keySecret>
        </properties>
    </profile>
  </profiles>
</settings>
```


## Table Store CSV loader

* setup table in Table Store console or in `otscli`

```bash
# CREATE TABLE
ct ots_chicago_crime o_ID:integer readrt:100 writert:50
```

* put csv in `src/main/resources/csv/`
* put column types in `src/main/resources/${table_name}.properties`
* prepare class which implements `TableStoreCsvParser.java`
* example is `src/main/java/com/example/CrimeParser.java`
* run `App.java`
* you can check the result by exporting table data

```
export ots_chicago_crime /tmp/tablecrime.txt
```

## Reference

* Table Store Java SDK document  
  https://www.alibabacloud.com/help/doc-detail/43005.htm
* maven setup  
  https://qiita.com/marrontan619/items/317b954b5f4535bd74da
