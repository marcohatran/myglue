# myglue
## How to  run
- ```mvn clean install```
- add to InteliJ configuration:
    VM Options: ```-Dspark.master=local[*] -Dspark.app.name=localrun```
  Program Arguments (if desired) ```--show=entourage```
  
## Run database
-  ```docker-compose up -d```
- Username: ```postgres```
- password: ```changeme```