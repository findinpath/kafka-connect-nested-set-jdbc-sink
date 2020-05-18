Testing hints for Oracle with testcontainers
============================================

Follow the guideline:

https://blogs.oracle.com/oraclemagazine/deliver-oracle-database-18c-express-edition-in-containers

for setting up an Oracle docker image.


The initial setup of the image (due to large downloads) will probably take around 30 mins.

Oracle docker images repository:

https://github.com/oracle/docker-images

Since the [Oracle-XE Module](https://www.testcontainers.org/modules/databases/oraclexe/)
from [testcontainers](https://www.testcontainers.org/) doesn't work properly,
a custom [OracleContainer](src/test/java/com/findinpath/connect/nestedset/jdbc/testcontainers/OracleContainer.java)
has been developed for test purposes.

The `system` user is a restricted Oracle user and when trying to connect with this user
the following exception will be thrown:

```
ORA-12526, TNS:listener: all appropriate instances are in restricted mode
``` 


Workaround:

Set a breakpoint on
`org.testcontainers.containers.JdbcDatabaseContainer.createConnection`

and after the database within the testcontainers test has started, execute the following script:

```bash
docker exec -it --user=oracle ${oracle_image_name} bash
``` 

```sqlplus
sqlplus /nolog
conn sys as sysdba
password: oracle
alter system disable restricted session;
```